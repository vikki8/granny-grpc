# GRANNY-gRPC setup

Full guide to set up the GRANNY runtime gRPC backend:

1. Install host dependencies
2. Bring up a 3-host dist-test cluster
3. Build the runtime
4. Upload Hotel Reservation WASM
5. Run a gRPC dist test with metrics logging

Assume the repository is checked out as `granny-grpc` (or clone it, then `cd` into your checkout).

---

## 1. Install dependencies

```bash
sudo apt-get update && sudo apt-get upgrade -y

sudo apt-get install -y \
  apt-transport-https \
  ca-certificates \
  curl \
  gnupg \
  lsb-release \
  software-properties-common \
  git \
  python3 \
  python3-pip \
  python3-venv

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] \
https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
```

Optional (so Docker works without `sudo`):

```bash
sudo usermod -aG docker "$USER"
# log out and back in for the group to apply
```

---

## 2. Enter the repo and make scripts executable

```bash
cd granny-grpc

chmod +x "$PWD"/clients/cpp/bin/*.sh
chmod +x "$PWD"/bin/*.sh
chmod +x "$PWD"/deploy/dist-test/*.sh
```

---

## 3. Activate the Faasm environment

```bash
source ./bin/workon.sh
export FAASM_INI_FILE=./faasm.ini
```

`workon.sh` creates or reuses a Python venv (for example `venv-bm`) and puts `faasmctl` on your `PATH`.

---

## 4. Patch `faasmctl` for the second dist-test server

Current `faasmctl` starts only one `dist-test-server`. So we add another
`dist-test-server-2` (3 hosts: master + 2 workers). Patch the installed package
once after the venv exists:

Find the file:

```bash
python -c 'import faasmctl.tasks.deploy as d; print(d.__file__)'
```

Open that file and change:

```python
run_compose_cmd(ini_file, "up -d dist-test-server")
```

to:

```python
run_compose_cmd(ini_file, "up -d dist-test-server dist-test-server-2")
```

Re-apply this after any `pip install` or upgrade of `faasmctl`, because it will overwrite the patch.

---

## 5. Deploy the dist-test cluster

```bash
faasmctl deploy.dist-tests --mount-source .
```

This writes `faasm.ini` and starts planner, Redis, MinIO, upload, `faasm-cli`, and both dist-test servers.

---

## 6. Build the Faasm / Faabric dist-test binaries

```bash
faasmctl cli.faasm
```

Inside the container:

```bash
./deploy/dist-test/build_internal.sh
exit
```

---

## 7. Populate local Faasm sysroot mounts

Copy the WASM toolchain (needed to compile Hotel functions) and the runtime root into `dev/faasm-local` on the host.

### Toolchain / LLVM sysroot

```bash
docker run --rm \
  -v "$PWD/dev/faasm-local:/out" \
  ghcr.io/faasm/cpp-sysroot:0.8.0 \
  bash -c 'cp -a /usr/local/faasm/toolchain /usr/local/faasm/llvm-sysroot /usr/local/faasm/native /out/ 2>/dev/null; ls -la /out'

ls dev/faasm-local/toolchain/tools/WasiToolchain.cmake
```

### Runtime root

```bash
CLI_IMAGE="$(grep FAASM_CLI_IMAGE .env 2>/dev/null | cut -d= -f2)"
CLI_IMAGE="${CLI_IMAGE:-ghcr.io/faasm/cli:$(cat VERSION)}"

docker run --rm \
  -v "$PWD/dev/faasm-local:/out" \
  "$CLI_IMAGE" \
  bash -c 'cp -a /usr/local/faasm/runtime_root /out/ && ls -la /out/runtime_root | head'

ls dev/faasm-local/runtime_root
```

---

## 8. Restart workers and upload WASM

```bash
faasmctl restart -s upload -s dist-test-server -s dist-test-server-2
./deploy/dist-test/upload.sh
```

`upload.sh` builds the Hotel Reservation WASM (via the cpp CLI) and uploads it to the cluster.

---

## 9. Run a gRPC dist test (example: hotel perf-adaptive) and append logs from all workers

```bash
mkdir -p grpc_metrics_out

faasmctl cli.faasm \
  --cmd "/build/faasm/bin/dist_tests '[hotel-perf-adaptive]'" \
  2>&1 | tee grpc_metrics_out/hotel_raw.log

faasmctl logs -s dist-test-server >> grpc_metrics_out/hotel_raw.log
faasmctl logs -s dist-test-server-2 >> grpc_metrics_out/hotel_raw.log
faasmctl logs -s planner >> grpc_metrics_out/hotel_raw.log
```

Other useful Catch2 filters:

```bash
faasmctl cli.faasm --cmd "/build/faasm/bin/dist_tests '[hotel-migrate-search]'"
faasmctl cli.faasm --cmd "/build/faasm/bin/dist_tests '[hotel-migrate-profile]'"
faasmctl cli.faasm --cmd "/build/faasm/bin/dist_tests '[hotel]'"
```

Perf-adaptive thresholds for the planner are set in `.env` (for example `PERF_LAT_P50_US`, `PERF_CPU_PCT`, `PERF_SUSTAIN_WINDOWS`, `PERF_COOLDOWN_MS`). Restart the planner or the full cluster after changing them.

---

## 10. Run latency & throughput graph plotting

```bash
python3 scripts/grpc_metrics/plot_publication_eval.py --log-dir grpc_metrics_out/logs --out-dir grpc_metrics_out/publication
```

---

## Quick reference

| Step | Command |
|------|---------|
| Env | `source ./bin/workon.sh && export FAASM_INI_FILE=./faasm.ini` |
| Deploy | `faasmctl deploy.dist-tests --mount-source .` |
| Build | `faasmctl cli.faasm` then `./deploy/dist-test/build_internal.sh` |
| Upload | `faasmctl restart -s upload -s dist-test-server -s dist-test-server-2 && ./deploy/dist-test/upload.sh` |
| Test | `faasmctl cli.faasm --cmd "/build/faasm/bin/dist_tests '[hotel-perf-adaptive]'"` |
| Tear down | `faasmctl delete` |
