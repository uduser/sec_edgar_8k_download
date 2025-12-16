"""
Simple Tkinter GUI for SEC_download.py

Windows friendly: paste CIKs or load from a file, set output folder and User-Agent, then start.
"""

from __future__ import annotations

import re
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext, ttk

from SEC_download import iter_ciks_from_file, run_download


def _split_ciks_text(text: str) -> list[str]:
    # allow comma/space/newline separated
    import re

    toks = re.split(r"[\s,]+", (text or "").strip())
    return [t for t in toks if t]


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("SEC EDGAR 8-K Downloader")
        self.geometry("900x700")

        self._running = False

        root = ttk.Frame(self, padding=12)
        root.pack(fill=tk.BOTH, expand=True)

        # User-Agent
        ua_frame = ttk.LabelFrame(root, text="User-Agent (必填，建議含 email)")
        ua_frame.pack(fill=tk.X, pady=(0, 10))
        self.ua_var = tk.StringVar(value="")
        ua_entry = ttk.Entry(ua_frame, textvariable=self.ua_var)
        ua_entry.pack(fill=tk.X, padx=8, pady=8)

        # CIK input
        cik_frame = ttk.LabelFrame(root, text="CIK 清單（可貼上；逗號/空白/換行分隔）")
        cik_frame.pack(fill=tk.BOTH, expand=False, pady=(0, 10))

        cik_header = ttk.Frame(cik_frame)
        cik_header.pack(fill=tk.X, padx=8, pady=(8, 0))
        self.cik_count_var = tk.StringVar(value="CIK 數量：0")
        ttk.Label(cik_header, textvariable=self.cik_count_var).pack(side=tk.LEFT)

        self.cik_text = scrolledtext.ScrolledText(cik_frame, height=8, wrap=tk.WORD)
        self.cik_text.pack(fill=tk.BOTH, expand=True, padx=8, pady=(6, 8))
        self.cik_text.bind("<KeyRelease>", lambda _e: self.update_cik_count())
        self.cik_text.bind("<<Paste>>", lambda _e: self.after(0, self.update_cik_count))

        btn_row = ttk.Frame(cik_frame)
        btn_row.pack(fill=tk.X, padx=8, pady=(0, 8))
        ttk.Button(btn_row, text="載入 CIK 檔案...", command=self.load_cik_file).pack(side=tk.LEFT)
        ttk.Button(btn_row, text="清空", command=self.clear_ciks).pack(side=tk.LEFT, padx=(8, 0))

        # Output folder
        out_frame = ttk.LabelFrame(root, text="輸出資料夾")
        out_frame.pack(fill=tk.X, pady=(0, 10))
        self.out_var = tk.StringVar(value=str((Path.cwd() / "downloads").resolve()))
        out_row = ttk.Frame(out_frame)
        out_row.pack(fill=tk.X, padx=8, pady=8)
        ttk.Entry(out_row, textvariable=self.out_var).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(out_row, text="選擇...", command=self.choose_out_dir).pack(side=tk.LEFT, padx=(8, 0))

        # Options
        opt_frame = ttk.LabelFrame(root, text="選項")
        opt_frame.pack(fill=tk.X, pady=(0, 10))

        self.include_amend_var = tk.BooleanVar(value=True)
        # GUI 固定使用「只抓主文件(primary) + EX-*，且只保留 .htm」模式；manifest 在此模式下不會寫出
        self.save_manifest_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(opt_frame, text="包含 8-K/A", variable=self.include_amend_var).pack(anchor="w", padx=8, pady=(6, 0))
        mcb = ttk.Checkbutton(opt_frame, text="每筆存 manifest.json（此模式不輸出）", variable=self.save_manifest_var)
        mcb.pack(anchor="w", padx=8, pady=(0, 6))
        mcb.state(["disabled"])

        tun_frame = ttk.Frame(opt_frame)
        tun_frame.pack(fill=tk.X, padx=8, pady=(0, 8))
        ttk.Label(tun_frame, text="min-interval (秒):").pack(side=tk.LEFT)
        self.min_interval_var = tk.DoubleVar(value=0.25)
        ttk.Entry(tun_frame, width=10, textvariable=self.min_interval_var).pack(side=tk.LEFT, padx=(6, 18))
        ttk.Label(tun_frame, text="max-workers:").pack(side=tk.LEFT)
        self.max_workers_var = tk.IntVar(value=2)
        ttk.Entry(tun_frame, width=10, textvariable=self.max_workers_var).pack(side=tk.LEFT, padx=(6, 0))

        date_frame = ttk.Frame(opt_frame)
        date_frame.pack(fill=tk.X, padx=8, pady=(0, 8))
        ttk.Label(date_frame, text="起始日期 (YYYY-MM-DD):").pack(side=tk.LEFT)
        self.start_date_var = tk.StringVar(value="2001-01-01")
        ttk.Entry(date_frame, width=14, textvariable=self.start_date_var).pack(side=tk.LEFT, padx=(6, 0))

        # Run controls
        run_frame = ttk.Frame(root)
        run_frame.pack(fill=tk.X, pady=(0, 10))
        self.start_btn = ttk.Button(run_frame, text="開始下載", command=self.start)
        self.start_btn.pack(side=tk.LEFT)
        self.status_var = tk.StringVar(value="Idle")
        ttk.Label(run_frame, textvariable=self.status_var).pack(side=tk.LEFT, padx=(10, 0))
        self.company_progress_var = tk.StringVar(value="公司進度：0/0（剩 0）")
        ttk.Label(run_frame, textvariable=self.company_progress_var).pack(side=tk.LEFT, padx=(12, 0))

        # Log
        log_frame = ttk.LabelFrame(root, text="進度 / Log")
        log_frame.pack(fill=tk.BOTH, expand=True)
        self.log_text = scrolledtext.ScrolledText(log_frame, height=16, wrap=tk.WORD, state=tk.NORMAL)
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        self._log("Ready.")
        self.update_cik_count()

    def _log(self, msg: str) -> None:
        self._update_company_progress_from_log(msg)
        self.log_text.insert(tk.END, msg + "\n")
        self.log_text.see(tk.END)

    def _log_threadsafe(self, msg: str) -> None:
        self.after(0, lambda: self._log(msg))

    def _update_company_progress_from_log(self, msg: str) -> None:
        # Expected prefix from core runner: "[idx/total] ..."
        m = re.match(r"^\[(\d+)/(\d+)\]\s+", msg)
        if not m:
            return
        idx = int(m.group(1))
        total = int(m.group(2))

        # When we're scanning company idx, previous companies are done (idx-1).
        done = idx - 1
        if "COMPANY_DONE" in msg:
            done = idx
        left = max(0, total - done)
        self.company_progress_var.set(f"公司進度：{done}/{total}（剩 {left}）")

    def clear_ciks(self) -> None:
        self.cik_text.delete("1.0", tk.END)
        self.update_cik_count()

    def update_cik_count(self) -> None:
        ciks = _split_ciks_text(self.cik_text.get("1.0", tk.END))
        self.cik_count_var.set(f"CIK 數量：{len(ciks)}")

    def load_cik_file(self) -> None:
        path = filedialog.askopenfilename(
            title="選擇 CIK 檔案",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
        )
        if not path:
            return
        try:
            ciks = iter_ciks_from_file(Path(path))
        except Exception as e:
            messagebox.showerror("讀取失敗", str(e))
            return
        if ciks:
            self.cik_text.insert(tk.END, ("\n" if self.cik_text.get("1.0", tk.END).strip() else "") + "\n".join(ciks))
        self.update_cik_count()
        self._log(f"Loaded {len(ciks)} CIKs from {path}")

    def choose_out_dir(self) -> None:
        path = filedialog.askdirectory(title="選擇輸出資料夾")
        if not path:
            return
        self.out_var.set(str(Path(path).resolve()))

    def _set_running(self, running: bool) -> None:
        self._running = running
        self.start_btn.configure(state=(tk.DISABLED if running else tk.NORMAL))
        self.status_var.set("Running..." if running else "Idle")

    def start(self) -> None:
        if self._running:
            return

        ua = self.ua_var.get().strip()
        out_dir = self.out_var.get().strip()
        ciks = _split_ciks_text(self.cik_text.get("1.0", tk.END))

        if not ua:
            messagebox.showerror("缺少資訊", "請輸入 User-Agent（建議含 email）。")
            return
        if not ciks:
            messagebox.showerror("缺少資訊", "請貼上或載入至少一個 CIK。")
            return
        if not out_dir:
            messagebox.showerror("缺少資訊", "請選擇輸出資料夾。")
            return

        include_amend = bool(self.include_amend_var.get())
        save_manifest = bool(self.save_manifest_var.get())
        start_date = self.start_date_var.get().strip()
        min_interval = float(self.min_interval_var.get())
        max_workers = int(self.max_workers_var.get())

        total_companies = len(ciks)
        self.company_progress_var.set(f"公司進度：0/{total_companies}（剩 {total_companies}）")

        self._set_running(True)
        self._log("Starting download ...")

        def worker() -> None:
            try:
                summary = run_download(
                    ciks=ciks,
                    out=out_dir,
                    user_agent=ua,
                    include_amendments=include_amend,
                    start_date=start_date,
                    min_interval=min_interval,
                    max_workers=max_workers,
                    save_manifest=save_manifest,
                    download_mode="primary_ex_htm",
                    log=self._log_threadsafe,
                )
                self.after(0, lambda: messagebox.showinfo("完成", f"完成！ok={summary['ok']} failed={summary['failed']}\n輸出：{summary['out']}"))
            except Exception as e:
                self._log_threadsafe(f"ERROR: {e}")
                self.after(0, lambda: messagebox.showerror("失敗", str(e)))
            finally:
                self.after(0, lambda: self._set_running(False))

        threading.Thread(target=worker, daemon=True).start()


def main() -> None:
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()


