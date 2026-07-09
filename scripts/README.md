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
├── start-flink-cluster.sh   # 啟動 Flink Session Cluster + HDFS
├── stop-flink-cluster.sh    # 停止並清理所有容器與網路
├── submit-job.sh            # 透過 Client 容器提交 Job
├── build-and-deploy.sh      # 一鍵構建並部署（Bash）
├── build-and-deploy.ps1     # 一鍵構建並部署（PowerShell）
└── generate_test_data.py    # 生成測試數據腳本
```

## 測試數據生成

使用 Python 腳本生成符合檢測規則的測試數據：

```bash
python scripts/generate_test_data.py
```

該腳本會生成多種測試場景：
1. 同一用戶連續 3 筆交易（觸發告警）
2. 間隔超過 1 分鐘的交易（不觸發）
3. 不同用戶的交易（不觸發）

