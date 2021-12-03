# avrotool

![Alt](https://repobeats.axiom.co/api/embed/377a1d5f89a2d9e278e710704e9431bff29512c5.svg "Repobeats analytics image")
[![GuardRails badge](https://api.guardrails.io/v2/badges/sangshuduo/avrotool.svg?token=cd10015d41dc47b92a9176f17fa71533e42d61992d522b1df19319785debc7ce&provider=github)](https://dashboard.guardrails.io/gh/sangshuduo/79732)
[![build](https://github.com/sangshuduo/avrotool/actions/workflows/build.yml/badge.svg?branch=develop)](https://github.com/sangshuduo/avrotool/actions/workflows/build.yml)

# install dependencies
apt install libjansson-dev

# build debug version
- For debug purpose
cmake .. -DCMAKE_BUILD_TYPE=DEBUG

- no debug info
cmake ..

# write data to avro example
./build/bin/avrotool -w w.avro -m ../sampledata/schema.json -d ../sampledata/data.csv

# read avro file
./build/bin/avrotool -r w.avro
