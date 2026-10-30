// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <filesystem>
#include <random>
#include <string>

class Scratch
{
public:
    explicit Scratch(const std::string& label)
    {
        std::random_device source;
        for (;;) {
            path_ = std::filesystem::temp_directory_path() /
                ("nosql-" + label + "-" + std::to_string(source()) + "-" + std::to_string(source()));
            if (std::filesystem::create_directory(path_))
                break;
        }
    }

    ~Scratch()
    {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    std::filesystem::path file(const std::string& name) const { return path_ / name; }

private:
    std::filesystem::path path_;
};