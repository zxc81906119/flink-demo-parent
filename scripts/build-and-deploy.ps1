# Flink 信用卡风控系统 - Windows PowerShell 构建部署脚本
# 使用方法: .\build-and-deploy.ps1

$ErrorActionPreference = "Stop"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " Flink 信用卡风控系统 - 构建部署脚本" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# 1. 清理并编译
Write-Host "`n[1/4] 清理并编译项目..." -ForegroundColor Yellow
mvn clean package -DskipTests

# 2. 检查 JAR 文件
$jarFile = "target\untitled1-1.0-SNAPSHOT.jar"
if (-not (Test-Path $jarFile)) {
    Write-Host "[ERROR] JAR 文件未生成，编译失败！" -ForegroundColor Red
    exit 1
}

Write-Host "[SUCCESS] JAR 文件生成成功: $jarFile" -ForegroundColor Green
Get-Item $jarFile | Select-Object Name, Length, LastWriteTime

# 3. 检查 Flink 集群是否运行
Write-Host "`n[2/4] 检查 Flink 集群状态..." -ForegroundColor Yellow

$jobmanagerExists = $false
try {
    $null = podman container exists flink-jobmanager 2>$null
    $jobmanagerExists = $LASTEXITCODE -eq 0
} catch {
    $jobmanagerExists = $false
}

if (-not $jobmanagerExists) {
    Write-Host "[WARNING] Flink 集群未运行，正在启动..." -ForegroundColor Yellow
    bash scripts/start-flink-cluster.sh
    Write-Host "[INFO] 等待 Flink 集群初始化 (10 秒)..." -ForegroundColor Yellow
    Start-Sleep -Seconds 10
} else {
    Write-Host "[INFO] Flink 集群已在运行" -ForegroundColor Green
}

# 4. 提交 Job
Write-Host "`n[3/4] 提交 Flink Job..." -ForegroundColor Yellow
bash scripts/submit-job.sh untitled1-1.0-SNAPSHOT.jar

Write-Host "`n==========================================" -ForegroundColor Cyan
Write-Host " 部署完成！" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host " Flink WebUI: http://localhost:8081" -ForegroundColor White
Write-Host " HDFS WebUI:  http://localhost:9870" -ForegroundColor White
Write-Host ""
Write-Host "后续步骤：" -ForegroundColor Yellow
Write-Host "  1. 访问 Flink WebUI 查看 Job 状态"
Write-Host "  2. 向 Kafka topic 'credit-card-transactions' 发送测试数据"
Write-Host "  3. 从 Kafka topic 'fraud-alerts' 消费告警事件"
Write-Host ""
Write-Host "生成测试数据：" -ForegroundColor Yellow
Write-Host "  python scripts\generate_test_data.py"
Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan

