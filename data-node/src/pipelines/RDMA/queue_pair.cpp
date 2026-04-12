//
// Created by Felix Zhang on 1/8/26.
//

#include <cassert>
#include <filesystem>

#include "device.h"
#include "queue_pair.h"

namespace fdl::rdma {

bool ibv_dev_is_equal_pci_addr(ibv_device* device, const char *pci_addr) {
	const char* name = ibv_get_device_name(device);
	const auto path = std::filesystem::path("/sys/class/infiniband") / name / "device";

	// The "device" entry is a symlink to the actual PCI device folder
	std::error_code error;
	if (std::filesystem::exists(path, error)) {
		const auto fullpath = std::filesystem::canonical(path, error).string();
		return fullpath.find(pci_addr) != std::string::npos;
	}

	return false;
}

bool capability_is_supported(ibv_context* context) {
	ibv_device_attr_ex attr{};
	if (const int error = ibv_query_device_ex(context, nullptr, &attr); error) {
		FDL_WARN("Failed to query device (%d): %s", error, strerror(error));
		return false;
	}

	return attr.orig_attr.atomic_cap == IBV_ATOMIC_HCA;
}

template<RangeOf<std::pair<char*, size_t>> Range>
int QueuePair::initialize(const Range& regions) {
	assert(initialized_ == false && regions.size() <= 4);

	auto& device = Device::instance();
	if (device.open() != 0) {
		FDL_WARN("[QP] failed to open IB device");
		return -EXIT_FAILURE;
	}

	psn_ = create_psn();
	port_num_ = device.next_port_num();
	max_rd_atomic_ = device.max_rd_atomic();

	int error = 0;

	// Create the necessary data structures for setting up a queue pair
	const int cqe = std::min(static_cast<int>(MAX_CQE), device.attr().max_cqe);
	cq_ = ibv_create_cq(device.context(), cqe, nullptr, nullptr, 0);
	if (!cq_) {
		error = errno;
		FDL_WARN("Failed to create completion queue (%d): %s", error, strerror(error));
		teardown();
		return error;
	}

	// Register memory regions
	constexpr int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
	for (size_t i = 0; i < regions.size(); ++i) {
		mrs_[i] = ibv_reg_mr(device.pd(), regions[i].first, regions[i].second, access);
		if (!mrs_[i]) {
			error = errno;
			FDL_WARN("Failed to register memory region (%d): %s", error, strerror(error));
			teardown();
			return error;
		}
	}

	// Create (RC) queue pair
	ibv_qp_init_attr init_attr{};
	init_attr.send_cq = cq_;
	init_attr.recv_cq = cq_;
	init_attr.cap = {
		.max_send_wr = MAX_INFLIGHT,
		.max_recv_wr = MAX_INFLIGHT,
		.max_send_sge = 2,
		.max_recv_sge = 2,
		.max_inline_data = MAX_INLINE_DATA,
	};
	init_attr.qp_type = IBV_QPT_RC;
	qp_ = ibv_create_qp(device.pd(), &init_attr);
	if (!qp_) {
		error = errno;
		FDL_WARN("Failed to create queue pair (%d): %s", error, strerror(error));
		teardown();
		return error;
	}

	// Transition queue pair to INIT state
	ibv_qp_attr attr{};
	attr.qp_state = IBV_QPS_INIT;
	attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
	attr.pkey_index = 0;
	attr.port_num = port_num_;
	error = ibv_modify_qp(qp_, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
	if (error) {
		FDL_WARN("Failed to transition QP state to INIT (%d): %s", error, strerror(error));
		teardown();
		return error;
	}

	FDL_INFO("Initialized QP: qp_num=%u, port_num=%u", qp_->qp_num, attr.port_num);
	initialized_ = true;

	return EXIT_SUCCESS;
}

int QueuePair::bringup(const ibv_qp_info& remote) {
	assert(initialized_ == true && connected_ == false);

	ibv_qp_attr attr{};

	// Transition the QP from the INIT state into the ready to receive (RTR) state
	attr.qp_state = IBV_QPS_RTR;

	// Configuration depends on link layer type
	ibv_port_attr port_attr{};
	ibv_query_port(Device::instance().context(), port_num_, &port_attr);
	if (port_attr.link_layer == IBV_LINK_LAYER_INFINIBAND) {
		attr.ah_attr.is_global = 0;
		attr.ah_attr.dlid = remote.lid;
	} else {
		attr.ah_attr.is_global = 1;
		attr.ah_attr.grh.dgid = remote.gid;
		attr.ah_attr.grh.sgid_index = gid_;
		attr.ah_attr.grh.hop_limit = 2;
		attr.ah_attr.grh.traffic_class = 0;
	}
	attr.ah_attr.port_num = port_num_;

	attr.path_mtu = port_attr.active_mtu;
	attr.dest_qp_num = remote.qp_num;
	attr.rq_psn = remote.psn;
	attr.max_dest_rd_atomic = std::min(max_rd_atomic_, remote.max_rd_atomic);
	attr.min_rnr_timer = 12;
	FDL_INFO("[QP] max_rd_atomic_=%u", max_rd_atomic_);

	int error = ibv_modify_qp(
		qp_, &attr, IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_AV | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER
	);
	if (error) {
		FDL_WARN("Failed to transition QP state to RTR (%d): %s", error, strerror(error));
		return error;
	}

	// Transition the QP into the ready to send (RTS) state
	attr.qp_state = IBV_QPS_RTS;
	attr.timeout = 14;
	attr.retry_cnt = 7;
	attr.rnr_retry = 7;
	attr.sq_psn = psn_;
	attr.max_rd_atomic = std::min(max_rd_atomic_, remote.max_rd_atomic);

	error = ibv_modify_qp(
		qp_, &attr, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC
	);
	if (error) {
		FDL_WARN("Failed to transition QP state to RTS (%d): %s", error, strerror(error));
		return error;
	}

	remote_ = remote;
	connected_ = true;

	return EXIT_SUCCESS;
}

void QueuePair::teardown() {
	// Guard against double-teardown
	if (!initialized_ && !connected_) {
		return;
	}

	// Mark the queue pair as un-initialized
	connected_ = false;
	initialized_ = false;

	if (qp_) {
		ibv_qp_attr attr{};
		attr.qp_state = IBV_QPS_ERR;
		if (const int error = ibv_modify_qp(qp_, &attr, IBV_QP_STATE); error) {
			FDL_WARN("[QP] teardown: failed to move QP to ERR state (%d): %s", error, strerror(error));
		}
	}

	if (cq_) {
		std::array<ibv_wc, POLL_BATCH_SIZE> wc_list{};
		int count;
		while ((count = ibv_poll_cq(cq_, POLL_BATCH_SIZE, wc_list.data())) > 0) {
			size_t completed = 0;
			for (int i = 0; i < count; ++i) {
				const auto &wc = wc_list[i];
				if (wc.opcode == IBV_WC_RECV || wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
					continue;
				}

				// Reclaim the slot count that was stored at submission
				completed += completions_[wc.wr_id].count.fetch_and(0, std::memory_order_acq_rel);
			}
			if (completed > 0) {
				inflight_.fetch_sub(completed, std::memory_order_acq_rel);
			}
		}
	}

	if (qp_) {
		if (const int error = ibv_destroy_qp(qp_); error) {
			FDL_WARN("[QP] teardown: ibv_destroy_qp failed (%d): %s", error, strerror(error));
		}
		qp_ = nullptr;
	}
	if (cq_) {
		if (const int error = ibv_destroy_cq(cq_); error) {
			FDL_WARN("[QP] teardown: ibv_destroy_cq failed (%d): %s", error, strerror(error));
		}
		cq_ = nullptr;
	}
	for (auto &mr: mrs_) {
		if (!mr) { continue; }
		if (const int error = ibv_dereg_mr(mr); error) {
			FDL_WARN("[QP] teardown: ibv_dereg_mr failed (%d): %s", error, strerror(error));
		}
		mr = nullptr;
	}

	inflight_.store(0, std::memory_order_relaxed);
	unsignaled_.store(0, std::memory_order_relaxed);
}

ibv_qp_info QueuePair::materialize() const {
	auto* context = Device::instance().context();
	assert(context != nullptr);

	ibv_port_attr port_attr{};
	ibv_query_port(context, port_num_, &port_attr);

	ibv_qp_info info = {
		{},
		qp_->qp_num,
		port_num_,
		psn_,
		max_rd_atomic_,
		{},
		{}
	};
	for (size_t i = 0; i < MAX_NUM_MR; ++i) {
		if (!mrs_[i]) { continue; }
		info.addr[i] = reinterpret_cast<uint64_t>(mrs_[i]->addr);
		info.rkey[i] = mrs_[i]->rkey;
	}

	if (port_attr.link_layer == IBV_LINK_LAYER_INFINIBAND) {
		info.lid = port_attr.lid;
	} else {
		ibv_query_gid(context, port_num_, gid_, &info.gid);
	}

	return info;
}

uint64_t QueuePair::write(const std::vector<ibv_write_t>& operations, bool signal, void* context) {
	if (context) { assert(signal); }

	// Wait for space
	const uint64_t ticket = reserve_slot(signal);
	completions_[ticket].context = context;

	return write_(ticket, operations, signal);
}

uint64_t QueuePair::write_(const uint64_t ticket, const std::vector<ibv_write_t>& operations, const bool signal) {
	// Prepare work requests for submission
	std::vector<ibv_send_wr> wr_list{operations.size()};
	std::vector<ibv_sge> sge_list{operations.size()};
	for (size_t i = 0; i < operations.size(); ++i) {
		const auto& [source, length, destination, local, remote] = operations[i];
		const auto [lkey, rkey] = get_keys(local, remote);

		sge_list[i] = {
			.addr = reinterpret_cast<uint64_t>(source),
			.length = length,
			.lkey = lkey
		};

		wr_list[i].wr_id = ticket;
		wr_list[i].sg_list = &sge_list[i];
		wr_list[i].num_sge = 1;
		wr_list[i].opcode = IBV_WR_RDMA_WRITE;
		wr_list[i].send_flags = 0;
		wr_list[i].wr.rdma.remote_addr = destination;
		wr_list[i].wr.rdma.rkey = rkey;
		wr_list[i].next = nullptr;

		if (length <= MAX_INLINE_DATA) {
			wr_list[i].send_flags |= IBV_SEND_INLINE;
		}
		if (i < operations.size() - 1) {
			wr_list[i].next = &wr_list[i + 1];
		}
	}

	if (signal) {
		wr_list.back().send_flags |= IBV_SEND_SIGNALED;
	}

	// Post work request to queue pair
	ibv_send_wr *bad_wr;
	if (const int ret = ibv_post_send(qp_, &wr_list[0], &bad_wr); ret) {
		FDL_WARN("ibv_post_send failed @ write: %s (%d), inflight_=%lu", strerror(ret), ret, inflight_.load(std::memory_order_acquire));
		throw std::runtime_error("");
	}

	return ticket;
}

/*
 * This expects that both the expected and desired values are already in network-byte order
 */
uint64_t QueuePair::compare_exchange(
	uint64_t* laddr, const uint64_t raddr, const uint64_t expected, const uint64_t desired, const mr_t l_mr, const mr_t r_mr, bool signal
) {
	auto [lkey, rkey] = get_keys(l_mr, r_mr);

	ibv_sge sg{};
	sg.addr = reinterpret_cast<uint64_t>(laddr);
	sg.length = sizeof(uint64_t);
	sg.lkey = lkey;

	// Wait for space to submit WR
	const uint64_t ticket = reserve_slot(signal);
	completions_[ticket].context = nullptr;

	ibv_send_wr wr{};
	wr.wr_id = ticket;
	wr.sg_list = &sg;
	wr.num_sge = 1;
	wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
	wr.send_flags = signal ? IBV_SEND_SIGNALED : 0;
	wr.wr.atomic = {
		.remote_addr = raddr,
		.compare_add = expected,
		.swap = desired,
		.rkey = rkey,
	};
	wr.next = nullptr;

	// Post work request to queue pair
	ibv_send_wr *bad_wr;
	if (const int ret = ibv_post_send(qp_, &wr, &bad_wr); ret) {
		FDL_WARN("ibv_post_send failed @ compare & swap: %s (%d)", strerror(ret), ret);
		throw std::runtime_error("");
	}

	return ticket;
}

size_t QueuePair::poll(std::array<size_t, POLL_BATCH_SIZE>& tickets, std::array<void*, POLL_BATCH_SIZE>& contexts) {
	if (!cq_) {
		return 0;
	}

	std::array<ibv_wc, POLL_BATCH_SIZE> wc_list{};
	const int count = ibv_poll_cq(cq_, POLL_BATCH_SIZE, wc_list.data());
	if (count < 0) {
		FDL_WARN("ibv_poll_cq failed: %s (%d)", strerror(errno), errno);
		return 0;
	}

	size_t outbound_completions = 0;
	size_t current = 0;
	for (int i = 0; i < count; ++i) {
		const auto& wc = wc_list[i];
		if (wc.status != IBV_WC_SUCCESS) {
			FDL_WARN("Work completion failed: wr_id=%lu status=%d (%s)", wc.wr_id, wc.status, ibv_wc_status_str(wc.status));
			continue;
		}

		// Resubmit WRs to the receive queue
		if (wc.opcode == IBV_WC_RECV || wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
			FDL_WARN("Unexpected completion opcode (%d)", wc.opcode);
			continue;
		}

		// Update outbound tracking statistics
		if (wc.opcode == IBV_WC_RDMA_WRITE || wc.opcode == IBV_WC_COMP_SWAP || wc.opcode == IBV_WC_RDMA_READ || wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
			tickets[current] = wc.wr_id;
			contexts[current] = completions_[wc.wr_id].context;
			++current;

			outbound_completions += completions_[wc.wr_id].count.fetch_and(0,  std::memory_order_acq_rel);

			continue;
		}

		// This is only reached if the operation was not handled
		FDL_WARN("Unexpected opcode: %s (%d)", ibv_wc_opcode_str(wc.opcode), wc.opcode);
	}

	if (outbound_completions > 0) {
		inflight_.fetch_sub(outbound_completions, std::memory_order_acq_rel);
	}

	return current;
}

// Explicitly instantiate template types
template int QueuePair::initialize<std::array<std::pair<char*, size_t>, 3>>(const std::array<std::pair<char*, size_t>, 3>&);
template int QueuePair::initialize<std::vector<std::pair<char*, size_t>>>(const std::vector<std::pair<char*, size_t>>&);

}