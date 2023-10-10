/*
* Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
* This source code is licensed under the BSD-style license found in the
* LICENSE file in the root directory of this source tree. An additional grant
* of patent rights can be found in the PATENTS file in the same directory.
*/

#include "io_thread.h"

namespace pikiwidb {

void PThread::Start() {
  event_loop_ = std::make_unique<EventLoop>();
  thread_ = std::thread([this]() {
    event_loop_->Init();
    event_loop_->Run();
  });
  state_ = State::kStarted;
}

void PThread::Stop() {
  State expected = State::kStarted;
  if (state_.compare_exchange_strong(expected, State::kStopped)) {
    event_loop_->Stop();
    if (thread_.joinable()) {
      thread_.join();
    }
  }
}

}