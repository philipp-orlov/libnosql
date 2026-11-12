# Build from the directory that holds both checkouts:  docker build -f libnosql/tests/Ubuntu2004.Dockerfile ..
FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive
RUN ["apt-get", "update"]
RUN ["apt-get", "install", "-y", "--no-install-recommends", "g++", "cmake", "make"]

WORKDIR /src
COPY libcore/ /src/libcore/
COPY libnosql/CMakeLists.txt /src/libnosql/
COPY libnosql/cmake/ /src/libnosql/cmake/
COPY libnosql/include/ /src/libnosql/include/
COPY libnosql/src/ /src/libnosql/src/
COPY libnosql/tests/ /src/libnosql/tests/
COPY libnosql/examples/ /src/libnosql/examples/
COPY libnosql/tools/ /src/libnosql/tools/

RUN ["g++", "--version"]
RUN ["cmake", "--version"]
RUN ["cmake", "-S", "/src/libnosql", "-B", "/build", "-DCMAKE_BUILD_TYPE=Release", "-DCMAKE_CXX_FLAGS=-pedantic-errors"]
RUN ["cmake", "--build", "/build", "--parallel", "4"]
RUN ["cmake", "-E", "chdir", "/build", "ctest", "--output-on-failure", "-j2"]
RUN ["cmake", "--install", "/build", "--prefix", "/opt/nosql"]
RUN ["cmake", "-S", "/src/libnosql/tests/InstalledConsumer", "-B", "/consumer", "-DCMAKE_PREFIX_PATH=/opt/nosql"]
RUN ["cmake", "--build", "/consumer", "--parallel", "4"]
RUN ["/consumer/consumer"]