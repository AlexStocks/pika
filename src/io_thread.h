/*
* Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
* This source code is licensed under the BSD-style license found in the
* LICENSE file in the root directory of this source tree. An additional grant
* of patent rights can be found in the PATENTS file in the same directory.
*/

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <set>
#include <thread>

#include "client.h"
#include "net/event_loop.h"

namespace pikiwidb {

class PThread : public std::enable_shared_from_this<PThread> {
 public:
  PThread() = default;
  virtual ~PThread() = default;

  PThread(const PThread&) = delete;
  void operator=(const PThread&) = delete;

  void Start();
  void Stop();

  void SetName(const std::string& name) { event_loop_->SetName(name); }
  const std::string& GetName() const { return event_loop_->GetName(); }

  EventLoop* GetEventLoop() const { return event_loop_.get(); }

 private:
  std::string name_;
  std::thread thread_;
  std::unique_ptr<EventLoop> event_loop_;

  PClient* current_client_ = nullptr;
  std::set<std::weak_ptr<PClient>, std::owner_less<std::weak_ptr<PClient> > >  monitors_;

  std::atomic<State> state_ = {State::kNone};
};

} // namespace pikiwidb
