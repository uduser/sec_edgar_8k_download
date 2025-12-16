# SEC EDGAR 8-K 下載器

本專案提供一個簡單的 Python 腳本 `SEC_download.py`，可依公司 **CIK** 清單查詢該公司歷史上的 **Form 8-K**（可選含 8-K/A），並下載每筆 filing 的檔案。

目前預設（GUI 與常用模式）為：

- **只抓主文件 (8-K/8-K/A) + EX-\***  
- **只保留 `.htm`**（不下載 `.jpg/.pdf/.json` 等附件）
- **可設定起始日期**（例如 `2001-01-01`）

## 安裝

```bash
python -m pip install -r requirements.txt
```

## 使用方式

### 直接指定 CIK

```bash
python SEC_download.py --ciks 0000320193 0001652044 --out downloads --user-agent "Your Name your.email@example.com" --start-date 2001-01-01
```

### 從檔案讀取 CIK（每行一個，或逗號/空白分隔）

```bash
python SEC_download.py --cik-file ciks.txt --out downloads --user-agent "Your Name your.email@example.com" --start-date 2001-01-01
```

### 只抓主文件 + EX-*，且只保留 .htm（建議）

GUI 版已固定使用此模式；CLI 可用：

```bash
python SEC_download.py --cik-file ciks.txt --out downloads --user-agent "Your Name your.email@example.com" --start-date 2001-01-01
```

## 視窗版（Windows / Tkinter）

直接執行：

```bash
python SEC_download_gui.py
```

在視窗中填入：

- **User-Agent**（必填，建議含 email）
- **CIK 清單**（貼上或載入檔案）
- **輸出資料夾**
- **起始日期**（預設 `2001-01-01`）

按「開始下載」即可。

## 資料來源與「全歷史」說明

- 本工具會優先使用 `data.sec.gov/submissions/CIK##########.json`（通常請求數較少、速度較快）
- 若該來源對某些公司 **缺少較早期資料**，且你設定的 `--start-date` 需要更早年份，會自動回退使用 `browse-edgar?action=getcompany&type=8-K` 補齊舊資料

## 打包成 EXE（GUI 版）

需要 Windows + Python 環境（建議同一台要發佈 EXE 的機器上打包）。

### PowerShell（建議）

```bash
pwsh -File .\build_exe.ps1
```

### CMD / bat

```bash
.\build_exe.bat
```

完成後 EXE 會在：

- `dist/SEC_8K_Downloader.exe`

## 重要說明（SEC Fair Access）

- **一定要提供** `--user-agent`（建議含聯絡 email），避免請求被拒絕。
- 下載量大時，請調整 `--min-interval` 與 `--max-workers`，降低觸發 429/403 的機率。
- 6000+ CIK 建議分批 / 分機器跑（若是不同出口 IP，通常能顯著加速）。

## 輸出結構

下載後會依下列方式建立資料夾：

`<out>/<CIK10>/<filingDate>_<accessionNo>/...`

例如：

`downloads/0000320193/2024-01-01_0000320193-24-000001/`


