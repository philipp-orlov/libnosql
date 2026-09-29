FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive
RUN ["apt-get", "update"]
RUN ["apt-get", "install", "-y", "--no-install-recommends", "g++", "cmake", "make"]

WORKDIR /src
COPY CMakeLists.txt ./
COPY cmake/ cmake/
COPY include/ include/
COPY src/ src/
COPY tests/ tests/
COPY examples/ examples/
COPY tools/ tools/

RUN ["g++", "--version"]
RUN ["cmake", "--version"]
RUN ["cmake", "-S", "/src", "-B", "/build", "-DCMAKE_BUILD_TYPE=Release", "-DCMAKE_CXX_FLAGS=-pedantic-errors"]
RUN ["cmake", "--build", "/build", "--parallel", "4"]
RUN ["cmake", "-E", "chdir", "/build", "ctest", "--output-on-failure", "-j2"]
RUN ["cmake", "--install", "/build", "--prefix", "/opt/nosql"]
RUN ["cmake", "-S", "/src/tests/InstalledConsumer", "-B", "/consumer", "-DCMAKE_PREFIX_PATH=/opt/nosql"]
RUN ["cmake", "--build", "/consumer", "--parallel", "4"]
RUN ["/consumer/consumer"]