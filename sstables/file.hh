/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <core/file.hh>
#include <core/reactor.hh>
#include "checked-file-impl.hh"
#include "stats.hh"

namespace sstables {

// A file that collects sstables stats
class sstables_file_impl : public file_impl {
    file _file;
    open_flags _oflags;

public:
    sstables_file_impl(file file, open_flags oflags)
        : _file(std::move(file))
        , _oflags(oflags) {}

    sstables_file_impl(const sstables_file_impl&) = delete;
    sstables_file_impl& operator=(const sstables_file_impl&) = delete;
    sstables_file_impl(sstables_file_impl&&) = default;
    sstables_file_impl& operator=(sstables_file_impl&&) = default;

    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override {
        return get_file_impl(_file)->write_dma(pos, buffer, len, pc);
    }

    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return get_file_impl(_file)->write_dma(pos, std::move(iov), pc);
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        return get_file_impl(_file)->read_dma(pos, buffer, len, pc);
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return get_file_impl(_file)->read_dma(pos, iov, pc);
    }

    virtual future<> flush(void) override {
        return get_file_impl(_file)->flush();
    }

    virtual future<struct stat> stat(void) override {
        return get_file_impl(_file)->stat();
    }

    virtual future<> truncate(uint64_t length) override {
        return get_file_impl(_file)->truncate(length);
    }

    virtual future<> discard(uint64_t offset, uint64_t length) override {
        return get_file_impl(_file)->discard(offset, length);
    }

    virtual future<> allocate(uint64_t position, uint64_t length) override {
        return get_file_impl(_file)->allocate(position, length);
    }

    virtual future<uint64_t> size(void) override {
        return get_file_impl(_file)->size();
    }

    virtual future<> close() override {
        return get_file_impl(_file)->close().then([this] {
            sstables_stats::submit_close(_oflags);
        });
    }

    virtual std::unique_ptr<file_handle_impl> dup() override {
        auto f = get_file_impl(_file)->dup();
        sstables_stats::submit_open(_oflags);
        return f;
    }

    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
        return get_file_impl(_file)->list_directory(std::move(next));
    }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) override {
        return get_file_impl(_file)->dma_read_bulk(offset, range_size, pc);
    }
};

inline file make_sstables_file(file f, open_flags oflags)
{
    sstables_stats::submit_open(oflags);
    return file(::make_shared<sstables_file_impl>(f, oflags));
}

future<file>
inline open_sstables_file_dma(sstring name, open_flags oflags, file_open_options options = {})
{
    return open_file_dma(name, oflags, options).then([&] (file f) {
        return make_ready_future<file>(make_sstables_file(f, oflags));
    });
}

inline file make_checked_sstables_file(const io_error_handler& error_handler, file f, open_flags oflags)
{
    return make_sstables_file(make_checked_file(error_handler, f), oflags);
}

future<file>
inline open_checked_sstables_file_dma(const io_error_handler& error_handler,
                                      sstring name, open_flags oflags,
                                      file_open_options options = {})
{
    return open_checked_file_dma(error_handler, name, oflags, options).then([oflags] (file f) {
        return make_ready_future<file>(make_sstables_file(f, oflags));
    });
}

}
