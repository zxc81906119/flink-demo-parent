# Flink Session Cluster (Podman)

本專案使用 Podman 部署 Apache Flink **Session 模式**叢集。

## 架構

| 容器 | 角色 | 說明 |
|------|------|------|
| `flink-jobmanager` | JobManager | 單一 JobManager (非 HA) |
| `flink-taskmanager-1` | TaskManager | 2 個 Task Slots |
| `flink-taskmanager-2` | TaskManager | 2 個 Task Slots |
| `flink-client` | Flink CLI Client | 掛載 `target/` 目錄，用於提交 Job |

- **Flink 版本**: 1.20.1 (LTS)
- **總並行度**: 4 slots (2 TaskManagers × 2 slots)

## 前置需求

- [Podman](https://podman.io/) 已安裝且可正常執行
- Bash shell (Linux / macOS / WSL / Git Bash)

## 使用方式

### 啟動叢集

```bash
chmod +x scripts/*.sh
./scripts/start-flink-cluster.sh
```

啟動後可開啟 Flink WebUI: http://localhost:8081

### 提交 Job

先將專案打包：

```bash
mvn clean package
```

再透過提交腳本提交 jar：

```bash
./scripts/submit-job.sh untitled1-1.0-SNAPSHOT.jar
```

或指定 main class：

```bash
./scripts/submit-job.sh untitled1-1.0-SNAPSHOT.jar --class org.example.Main
```

### 停止叢集

```bash
./scripts/stop-flink-cluster.sh
```

## 目錄結構

```
scripts/
├── start-flink-cluster.sh   # 啟動 Flink Session Cluster
├── stop-flink-cluster.sh    # 停止並清理所有容器與網路
└── submit-job.sh            # 透過 Client 容器提交 Job
```

