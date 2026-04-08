"""
TaxTrack GUI Launcher (Wizard).

Data lives in a workspace folder, not inside the repo:
  <workspace>/customers/<customer_slug>/{info.json,wallets.json,inbox/,reports/}
"""

from __future__ import annotations

import os
import platform
import queue
import subprocess
import sys
import threading
import traceback
import json
from datetime import datetime, timezone
from pathlib import Path
from tkinter import (
    BOTH,
    E,
    END,
    NW,
    W,
    X,
    IntVar,
    Listbox,
    StringVar,
    Text,
    Tk,
    messagebox,
)
from tkinter import filedialog, ttk

import taxtrack as _taxtrack_pkg

from taxtrack.customer.create_customer import create_customer_files, normalize_customer_folder

CHAIN_IDS = ("eth", "arb", "op", "base", "avax", "matic", "bnb", "ftm")
DEFAULT_CHAINS = {"eth", "arb", "op", "base", "avax"}

STEP_CUSTOMER = 1
STEP_WALLETS = 2
STEP_CHAINS = 3
STEP_SUMMARY = 4
STEP_PROCESS = 5

BG = "#FFFFFF"
PRIMARY_DARK = "#1f2937"
PRIMARY_DARK_ACTIVE = "#374151"
PRIMARY_DISABLED_BG = "#9ca3af"
LOG_BG = "#FAFAFA"
FONT_UI = ("Segoe UI", 10)
FONT_UI_BOLD = ("Segoe UI", 10, "bold")
FONT_MONO = ("Consolas", 9)

DEFAULT_WORKSPACE_ROOT = Path(r"C:\Users\zenin\Documents\taxtrack_loop")


def _default_workspace_root() -> Path:
    """
    Prefer the folder next to the EXE when frozen, otherwise the configured default.
    This makes the app behave like a real desktop product: data lives next to the executable.
    """
    try:
        if getattr(sys, "frozen", False):
            return Path(sys.executable).resolve().parent
    except Exception:
        pass
    return DEFAULT_WORKSPACE_ROOT


def _settings_path(workspace_root: Path) -> Path:
    return Path(workspace_root) / "taxtrack_settings.json"


def _taxtrack_root() -> Path:
    return Path(_taxtrack_pkg.__file__).resolve().parent


def _repo_root() -> Path:
    return _taxtrack_root().parent


def _valid_wallet(addr: str) -> bool:
    a = (addr or "").strip().lower()
    return bool(a.startswith("0x") and len(a) >= 10)


def _open_path(path: Path) -> None:
    path = path.resolve()
    if not path.exists():
        messagebox.showwarning("TaxTrack", f"Pfad existiert nicht:\n{path}")
        return
    try:
        if platform.system() == "Windows":
            os.startfile(str(path))  # type: ignore[attr-defined]
        elif platform.system() == "Darwin":
            subprocess.run(["open", str(path)], check=False)
        else:
            subprocess.run(["xdg-open", str(path)], check=False)
    except Exception as e:
        messagebox.showerror("TaxTrack", str(e))


def _apply_global_style(root: Tk) -> ttk.Style:
    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except Exception:
        pass

    style.configure(".", background=BG, font=FONT_UI)
    style.configure("TFrame", background=BG)
    style.configure("TLabel", background=BG, font=FONT_UI)
    style.configure("TLabelFrame", background=BG, relief="solid", borderwidth=1)
    style.configure("TLabelFrame.Label", background=BG, font=FONT_UI_BOLD, foreground="#111827")
    style.configure("TEntry", fieldbackground=BG, insertwidth=1)
    style.configure("TCheckbutton", background=BG, font=FONT_UI)
    style.map("TCheckbutton", background=[("active", BG), ("selected", BG)])
    style.configure("TScrollbar", background="#E5E7EB", troughcolor=BG, arrowcolor="#374151")
    style.configure(
        "Horizontal.TProgressbar",
        troughcolor="#E5E7EB",
        background=PRIMARY_DARK,
    )

    style.configure(
        "Primary.TButton",
        background=PRIMARY_DARK,
        foreground="#FFFFFF",
        font=FONT_UI_BOLD,
        padding=(24, 10),
        anchor="center",
    )
    style.map(
        "Primary.TButton",
        background=[
            ("active", PRIMARY_DARK_ACTIVE),
            ("pressed", "#111827"),
            ("disabled", PRIMARY_DISABLED_BG),
        ],
        foreground=[("disabled", "#F3F4F6")],
    )

    root.configure(bg=BG)
    return style


class TaxTrackLauncher(Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("KryptoBilanz")
        self.geometry("700x600")
        self.minsize(640, 520)

        _apply_global_style(self)

        self._log_queue: queue.Queue[str] = queue.Queue()
        self._busy = False
        self._current_step = STEP_CUSTOMER
        self._report_pdf_path: Path | None = None
        self._workspace_root = _default_workspace_root()
        self._customers_root = self._workspace_root / "customers"
        self._selected_customer_dir: Path | None = None

        self._step_frames: dict[int, ttk.Frame] = {}

        # Load workspace override if present (best-effort)
        self._load_settings()

        self._build_ui()
        self.after(120, self._drain_log_queue)
        self.show_frame(STEP_CUSTOMER)

    def _build_ui(self) -> None:
        outer = ttk.Frame(self, padding=14)
        outer.pack(fill=BOTH, expand=True)
        outer.rowconfigure(1, weight=1)
        outer.columnconfigure(0, weight=1)

        self.var_step_hint = StringVar(value="")
        ttk.Label(outer, textvariable=self.var_step_hint, font=FONT_UI_BOLD, foreground="#374151").grid(
            row=0, column=0, sticky=W, pady=(0, 8)
        )

        self._stack = ttk.Frame(outer)
        self._stack.grid(row=1, column=0, sticky="nsew")
        self._stack.rowconfigure(0, weight=1)
        self._stack.columnconfigure(0, weight=1)

        # Gemeinsame Datenfelder (an Frames angebunden)
        self.var_customer_mode = IntVar(value=0)  # 0=new, 1=existing
        self.var_wallet_mode = IntVar(value=0)  # 0=new, 1=existing
        self.var_existing_customer = StringVar(value="")

        self.var_company = StringVar(value="")
        self.var_name = StringVar(value="")
        self.var_year = StringVar(value=str(datetime.now().year))
        self.var_wallet = StringVar()
        self._chain_vars: dict[str, IntVar] = {}

        self._build_step_customer()
        self._build_step_wallets()
        self._build_step_chains()
        self._build_step_summary()
        self._build_step_process()

    def _update_step_title(self) -> None:
        titles = {
            STEP_CUSTOMER: "Schritt 1 von 5 — Kunde",
            STEP_WALLETS: "Schritt 2 von 5 — Wallets",
            STEP_CHAINS: "Schritt 3 von 5 — Chains & Jahr",
            STEP_SUMMARY: "Schritt 4 von 5 — Zusammenfassung",
            STEP_PROCESS: "Schritt 5 von 5 — Verarbeitung",
        }
        self.var_step_hint.set(titles.get(self._current_step, ""))

    def _load_settings(self) -> None:
        p = _settings_path(self._workspace_root)
        if not p.exists():
            return
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return
        if isinstance(data, dict):
            ws = data.get("workspace_root")
            if isinstance(ws, str) and ws.strip():
                self._set_workspace_root(Path(ws))

    def _save_settings(self) -> None:
        try:
            self._workspace_root.mkdir(parents=True, exist_ok=True)
            payload = {"workspace_root": str(self._workspace_root.resolve())}
            _settings_path(self._workspace_root).write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            # settings should never break the GUI
            pass

    def _set_workspace_root(self, workspace_root: Path) -> None:
        self._workspace_root = Path(workspace_root)
        self._customers_root = self._workspace_root / "customers"
        self._selected_customer_dir = None
        try:
            self.var_workspace_label.set(f"Workspace: {self._workspace_root}")
        except Exception:
            pass
        self._refresh_existing_customers()

    def _choose_workspace(self) -> None:
        try:
            selected = filedialog.askdirectory(
                title="Workspace auswählen",
                initialdir=str(self._workspace_root),
                mustexist=True,
            )
        except Exception as e:
            messagebox.showerror("Workspace", str(e))
            return
        if not selected:
            return
        self._set_workspace_root(Path(selected))
        self._save_settings()

    def _build_step_customer(self) -> None:
        f = ttk.Frame(self._stack, padding=16)
        f.grid(row=0, column=0, sticky="nsew")
        self._step_frames[STEP_CUSTOMER] = f
        f.columnconfigure(1, weight=1)

        ws_row = ttk.Frame(f)
        ws_row.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 10))
        ws_row.columnconfigure(0, weight=1)
        self.var_workspace_label = StringVar(value=f"Workspace: {self._workspace_root}")
        ttk.Label(ws_row, textvariable=self.var_workspace_label).grid(row=0, column=0, sticky=W)
        ttk.Button(ws_row, text="Workspace wählen…", command=self._choose_workspace).grid(
            row=0, column=1, sticky="e"
        )

        mode_row = ttk.Frame(f)
        mode_row.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 10))
        ttk.Radiobutton(
            mode_row,
            text="Neu",
            value=0,
            variable=self.var_customer_mode,
            command=self._on_customer_mode_change,
        ).pack(side="left", padx=(0, 12))
        ttk.Radiobutton(
            mode_row,
            text="Vorhanden",
            value=1,
            variable=self.var_customer_mode,
            command=self._on_customer_mode_change,
        ).pack(side="left")

        self.row_existing_customer = ttk.Frame(f)
        self.row_existing_customer.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(0, 10))
        self.row_existing_customer.columnconfigure(1, weight=1)
        ttk.Label(self.row_existing_customer, text="Kunde").grid(row=0, column=0, sticky=W, padx=(0, 12))
        self.cmb_existing_customer = ttk.Combobox(
            self.row_existing_customer,
            textvariable=self.var_existing_customer,
            state="readonly",
            values=[],
        )
        self.cmb_existing_customer.grid(row=0, column=1, sticky="ew")
        ttk.Button(
            self.row_existing_customer,
            text="Aktualisieren",
            command=self._refresh_existing_customers,
        ).grid(row=0, column=2, padx=(8, 0))

        ttk.Label(f, text="Firma (optional)").grid(row=3, column=0, sticky=W, padx=(0, 12), pady=6)
        ttk.Entry(f, textvariable=self.var_company, width=52).grid(row=3, column=1, sticky="ew", pady=6)

        ttk.Label(f, text="Name").grid(row=4, column=0, sticky=W, padx=(0, 12), pady=6)
        ttk.Entry(f, textvariable=self.var_name, width=52).grid(row=4, column=1, sticky="ew", pady=6)

        ttk.Label(f, text="Adresse").grid(row=5, column=0, sticky=NW, padx=(0, 12), pady=6)
        self.txt_address = Text(
            f,
            width=1,
            height=6,
            wrap="word",
            font=FONT_UI,
            bg=BG,
            relief="flat",
            highlightthickness=1,
            highlightbackground="#E5E7EB",
            highlightcolor="#9CA3AF",
            padx=8,
            pady=6,
        )
        self.txt_address.grid(row=5, column=1, sticky="nsew", pady=6)
        f.rowconfigure(5, weight=1)

        ttk.Button(f, text="Weiter", style="Primary.TButton", command=self._from_customer_next).grid(
            row=6, column=1, sticky="e", pady=(16, 0)
        )

        self._refresh_existing_customers()
        self._on_customer_mode_change()

    def _build_step_wallets(self) -> None:
        f = ttk.Frame(self._stack, padding=16)
        f.grid(row=0, column=0, sticky="nsew")
        self._step_frames[STEP_WALLETS] = f
        f.columnconfigure(0, weight=1)
        f.rowconfigure(2, weight=1)

        row_in = ttk.Frame(f)
        row_in.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        row_in.columnconfigure(0, weight=1)
        ttk.Entry(row_in, textvariable=self.var_wallet).grid(row=0, column=0, sticky="ew", padx=(0, 8))
        ttk.Button(row_in, text="Hinzufügen", command=self._add_wallet).grid(row=0, column=1)

        ttk.Button(f, text="Entfernen", command=self._remove_wallet).grid(row=1, column=0, sticky=W, pady=(0, 8))

        list_frame = ttk.Frame(f)
        list_frame.grid(row=2, column=0, sticky="nsew")
        list_frame.columnconfigure(0, weight=1)
        list_frame.rowconfigure(0, weight=1)

        sb = ttk.Scrollbar(list_frame, orient="vertical")
        sb.grid(row=0, column=1, sticky="ns")
        self.lb_wallets = Listbox(
            list_frame,
            height=8,
            font=FONT_UI,
            bg=BG,
            selectbackground="#E0E7FF",
            selectforeground="#111827",
            activestyle="none",
            selectmode="extended",
            exportselection=0,
            yscrollcommand=sb.set,
            highlightthickness=1,
            highlightbackground="#E5E7EB",
        )
        self.lb_wallets.grid(row=0, column=0, sticky="nsew")
        sb.config(command=self.lb_wallets.yview)

        hint_row = ttk.Frame(list_frame)
        hint_row.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(6, 0))
        hint_row.columnconfigure(0, weight=1)
        ttk.Label(
            hint_row,
            text="Lauf: nur markierte Wallets (Strg+Klick mehrere). Keine Markierung = alle. ",
            font=("Segoe UI", 9),
            foreground="#4b5563",
            wraplength=400,
            justify="left",
        ).grid(row=0, column=0, sticky="w")
        ttk.Button(hint_row, text="Markierung aufheben", command=self._clear_wallet_list_selection).grid(
            row=0, column=1, sticky="e", padx=(8, 0)
        )

        nav = ttk.Frame(f)
        nav.grid(row=3, column=0, sticky="ew", pady=(16, 0))
        nav.columnconfigure(1, weight=1)
        ttk.Button(nav, text="Zurück", command=self._from_wallets_back).grid(row=0, column=0, sticky=W)
        ttk.Button(nav, text="Weiter", style="Primary.TButton", command=self._from_wallets_next).grid(
            row=0, column=2, sticky="e"
        )

        # Wallet mode row (new vs existing)
        mode_row = ttk.Frame(f)
        mode_row.grid(row=4, column=0, sticky="ew", pady=(10, 0))
        ttk.Radiobutton(
            mode_row,
            text="Wallets: Neu",
            value=0,
            variable=self.var_wallet_mode,
            command=self._on_wallet_mode_change,
        ).pack(side="left", padx=(0, 12))
        ttk.Radiobutton(
            mode_row,
            text="Wallets: Vorhanden",
            value=1,
            variable=self.var_wallet_mode,
            command=self._on_wallet_mode_change,
        ).pack(side="left")
        self._on_wallet_mode_change()

    def _build_step_chains(self) -> None:
        f = ttk.Frame(self._stack, padding=16)
        f.grid(row=0, column=0, sticky="nsew")
        self._step_frames[STEP_CHAINS] = f

        lf = ttk.LabelFrame(f, text=" Chains ", padding=12)
        lf.pack(fill=X, pady=(0, 12))

        for i, cid in enumerate(CHAIN_IDS):
            v = IntVar(value=1 if cid in DEFAULT_CHAINS else 0)
            self._chain_vars[cid] = v
            ttk.Checkbutton(lf, text=cid, variable=v).grid(
                row=i // 4,
                column=i % 4,
                sticky=W,
                padx=(0, 12),
                pady=4,
            )

        row_y = ttk.Frame(f)
        row_y.pack(fill=X, pady=(0, 8))
        ttk.Label(row_y, text="Jahr").pack(side="left", padx=(0, 12))
        ttk.Entry(row_y, textvariable=self.var_year, width=12).pack(side="left")

        nav = ttk.Frame(f)
        nav.pack(fill=X, pady=(16, 0))
        nav.columnconfigure(1, weight=1)
        ttk.Button(nav, text="Zurück", command=self._from_chains_back).grid(row=0, column=0, sticky=W)
        ttk.Button(nav, text="Weiter", style="Primary.TButton", command=self._from_chains_next).grid(
            row=0, column=2, sticky=E
        )

    def _build_step_summary(self) -> None:
        f = ttk.Frame(self._stack, padding=16)
        f.grid(row=0, column=0, sticky="nsew")
        self._step_frames[STEP_SUMMARY] = f
        f.rowconfigure(0, weight=1)
        f.columnconfigure(0, weight=1)

        self.txt_summary = Text(
            f,
            width=1,
            height=1,
            wrap="word",
            font=FONT_UI,
            bg=LOG_BG,
            state="disabled",
            relief="flat",
            highlightthickness=1,
            highlightbackground="#E5E7EB",
            padx=12,
            pady=12,
        )
        self.txt_summary.grid(row=0, column=0, sticky="nsew")

        nav = ttk.Frame(f)
        nav.grid(row=1, column=0, sticky="ew", pady=(16, 0))
        nav.columnconfigure(1, weight=1)
        ttk.Button(nav, text="Zurück", command=self._from_summary_back).grid(row=0, column=0, sticky=W)
        ttk.Button(nav, text="Preflight Scan", command=self._from_summary_scan).grid(row=0, column=1, sticky=E, padx=(0, 8))
        ttk.Button(nav, text="Report erstellen", style="Primary.TButton", command=self._from_summary_run).grid(row=0, column=2, sticky=E)

    def _build_step_process(self) -> None:
        f = ttk.Frame(self._stack, padding=16)
        f.grid(row=0, column=0, sticky="nsew")
        self._step_frames[STEP_PROCESS] = f
        f.rowconfigure(2, weight=1)
        f.columnconfigure(0, weight=1)

        self.var_process_status = StringVar(value="")
        ttk.Label(f, textvariable=self.var_process_status, font=FONT_UI_BOLD, foreground="#111827").grid(
            row=0, column=0, sticky=W, pady=(0, 8)
        )

        self._progress = ttk.Progressbar(f, mode="indeterminate", length=400)
        self._progress.grid(row=1, column=0, sticky="ew", pady=(0, 12))

        log_frame = ttk.Frame(f)
        log_frame.grid(row=2, column=0, sticky="nsew")
        log_frame.rowconfigure(0, weight=1)
        log_frame.columnconfigure(0, weight=1)

        sb_log = ttk.Scrollbar(log_frame, orient="vertical")
        sb_log.grid(row=0, column=1, sticky="ns")
        self.txt_log = Text(
            log_frame,
            height=12,
            state="disabled",
            wrap="word",
            font=FONT_MONO,
            bg=LOG_BG,
            fg="#111827",
            relief="flat",
            highlightthickness=1,
            highlightbackground="#E5E7EB",
            padx=10,
            pady=8,
            yscrollcommand=sb_log.set,
        )
        self.txt_log.grid(row=0, column=0, sticky="nsew")
        sb_log.config(command=self.txt_log.yview)

        self.btn_open = ttk.Button(f, text="Öffnen", command=self._on_open_report, state="disabled")
        self.btn_open.grid(row=3, column=0, sticky="e", pady=(12, 0))

        self.var_status = StringVar(value="")
        ttk.Label(f, textvariable=self.var_status, wraplength=620, justify="left").grid(
            row=4, column=0, sticky=W, pady=(8, 0)
        )

    def _refresh_summary(self) -> None:
        name = (self.var_name.get() or "").strip()
        company = (self.var_company.get() or "").strip()
        try:
            addr = self.txt_address.get("1.0", END).strip()
        except Exception:
            addr = ""
        wallets = self._collect_wallets()
        chains = self._get_selected_chains()
        year_s = (self.var_year.get() or "").strip()
        customer_dir = str(self._selected_customer_dir) if self._selected_customer_dir else "—"

        lines = [
            f"Kundenordner:\n{customer_dir}\n",
            f"Firma:\n{company or '—'}\n",
            f"Name:\n{name}\n",
            f"Adresse:\n{addr}\n",
            "Wallets:",
            *(f"  • {w}" for w in wallets),
            "",
            f"Chains: {', '.join(chains) if chains else '—'}",
            "",
            f"Jahr: {year_s}",
        ]
        text = "\n".join(lines)
        self.txt_summary.configure(state="normal")
        self.txt_summary.delete("1.0", END)
        self.txt_summary.insert("1.0", text)
        self.txt_summary.configure(state="disabled")

    def _from_customer_next(self) -> None:
        if self.var_customer_mode.get() == 0:
            if not (self.var_name.get() or "").strip():
                messagebox.showwarning("Kunde", "Bitte einen Namen eingeben.")
                return
            # prepare customer folder; handle collisions
            slug = normalize_customer_folder(self.var_name.get())
            target_dir = self._customers_root / slug
            if target_dir.exists():
                use_existing = messagebox.askyesno(
                    "Kunde vorhanden",
                    f"Der Kunde-Ordner existiert bereits:\n{target_dir}\n\nAls vorhandenen Kunden verwenden?",
                )
                if use_existing:
                    self._selected_customer_dir = target_dir
                else:
                    return
            else:
                self._selected_customer_dir = target_dir
        else:
            # existing
            self._refresh_existing_customers()
            chosen = (self.var_existing_customer.get() or "").strip()
            if not chosen:
                messagebox.showwarning("Kunde", "Bitte einen vorhandenen Kunden auswählen.")
                return
            self._selected_customer_dir = self._customers_root / chosen
            if not self._selected_customer_dir.is_dir():
                messagebox.showwarning("Kunde", "Ausgewählter Kundenordner nicht gefunden.")
                return
            self._load_customer_profile(self._selected_customer_dir)

        # persist profile (creates folder + info.json) if new or updated
        try:
            self._ensure_customer_profile_saved()
        except Exception as e:
            messagebox.showerror("Kunde", str(e))
            return
        self.show_frame(STEP_WALLETS)

    def _from_wallets_back(self) -> None:
        self.show_frame(STEP_CUSTOMER)

    def _from_wallets_next(self) -> None:
        if not self._collect_wallets():
            messagebox.showwarning("Wallets", "Bitte mindestens eine Wallet hinzufügen.")
            return
        try:
            self._save_wallets_json()
        except Exception as e:
            messagebox.showerror("Wallets", str(e))
            return
        self.show_frame(STEP_CHAINS)

    def _from_chains_back(self) -> None:
        self.show_frame(STEP_WALLETS)

    def _from_chains_next(self) -> None:
        try:
            int((self.var_year.get() or "").strip())
        except ValueError:
            messagebox.showwarning("Jahr", "Bitte ein gültiges Jahr eingeben.")
            return
        if not self._get_selected_chains():
            messagebox.showwarning("Chains", "Bitte mindestens eine Chain auswählen.")
            return
        self.show_frame(STEP_SUMMARY)

    def _from_summary_back(self) -> None:
        self.show_frame(STEP_CHAINS)

    def _from_summary_run(self) -> None:
        if self._busy:
            messagebox.showinfo("KryptoBilanz", "Ein Lauf ist bereits aktiv.")
            return
        name = (self.var_name.get() or "").strip()
        wallets = self._collect_wallets()
        try:
            year = int((self.var_year.get() or "").strip())
        except ValueError:
            messagebox.showwarning("Eingabe", "Jahr ist ungültig.")
            return
        chains = self._get_selected_chains()
        address_text = ""
        try:
            address_text = self.txt_address.get("1.0", END).strip()
        except Exception:
            address_text = ""

        self._report_pdf_path = None
        self.show_frame(STEP_PROCESS)

        self.txt_log.configure(state="normal")
        self.txt_log.delete("1.0", END)
        self.txt_log.configure(state="disabled")
        self.var_status.set("")
        self.var_process_status.set("Downloading...")
        self.btn_open.configure(state="disabled")
        self._progress.configure(mode="indeterminate")
        self._progress.start(10)

        self._busy = True

        def work() -> None:
            try:
                self._run_pipeline_safe(name, address_text, year, wallets, chains)
            finally:

                def done() -> None:
                    self._busy = False
                    try:
                        self._progress.stop()
                    except Exception:
                        pass

                self.after(0, done)

        threading.Thread(target=work, daemon=True).start()

    def _from_summary_scan(self) -> None:
        """
        Safe preflight scan:
        - downloads wallet inbox data (same as report)
        - runs preflight_scan module (no PDF/CSV output)
        - writes reports/preflight_scan_<year>.json
        """
        if self._busy:
            messagebox.showinfo("KryptoBilanz", "Ein Lauf ist bereits aktiv.")
            return
        wallets = self._collect_wallets()
        if not wallets:
            messagebox.showwarning("Wallets", "Bitte mindestens eine Wallet auswählen.")
            return
        try:
            year = int((self.var_year.get() or "").strip())
        except ValueError:
            messagebox.showwarning("Eingabe", "Jahr ist ungültig.")
            return
        chains = self._get_selected_chains()
        if not chains:
            messagebox.showwarning("Chains", "Bitte mindestens eine Chain auswählen.")
            return
        name = (self.var_name.get() or "").strip()
        address_text = ""
        try:
            address_text = self.txt_address.get("1.0", END).strip()
        except Exception:
            address_text = ""

        self._report_pdf_path = None
        self.show_frame(STEP_PROCESS)
        self.txt_log.configure(state="normal")
        self.txt_log.delete("1.0", END)
        self.txt_log.configure(state="disabled")
        self.var_status.set("")
        self.var_process_status.set("Preflight: Downloading...")
        self.btn_open.configure(state="disabled")
        self._progress.configure(mode="indeterminate")
        self._progress.start(10)
        self._busy = True

        def work() -> None:
            try:
                self._run_preflight_safe(name, address_text, year, wallets, chains)
            finally:
                def done() -> None:
                    self._busy = False
                    try:
                        self._progress.stop()
                    except Exception:
                        pass
                self.after(0, done)

        threading.Thread(target=work, daemon=True).start()

    def _run_preflight_safe(
        self,
        display_name: str,
        address: str,
        year: int,
        wallet_addrs: list[str],
        chains: list[str],
    ) -> None:
        repo_root = _repo_root()
        customer_dir = self._selected_customer_dir
        if customer_dir is None:
            self.after(0, lambda: messagebox.showerror("KryptoBilanz", "Kein Kundenordner ausgewählt."))
            return
        slug = customer_dir.name
        chains_csv = ",".join(chains)
        wallets_json_path = customer_dir / "wallets.json"
        original_wallets_json: str | None = None

        try:
            self.after(0, lambda: self._set_phase_status("Preflight: Downloading..."))

            # Keep wallets.json aligned with selected subset for this scan only.
            if self.var_wallet_mode.get() == 1 and wallets_json_path.exists():
                try:
                    original_wallets_json = wallets_json_path.read_text(encoding="utf-8")
                except Exception:
                    original_wallets_json = None
            self._save_wallets_json()

            inbox_root = customer_dir / "inbox"
            for i, w in enumerate(wallet_addrs, start=1):
                self._log(f"[1/2] Download Wallet {i}/{len(wallet_addrs)}: {w} …")
                code = self._run_python_module(
                    cwd=repo_root,
                    module="taxtrack.root.download_wallet",
                    args=["--wallet", w, "--chains", chains_csv, "--inbox", str(inbox_root)],
                )
                if code != 0:
                    self._log(f"[1/2][WARN] download_wallet Exit-Code {code}")

            self.after(0, lambda: self._set_phase_status("Preflight: Scanning..."))
            self._log("[2/2] preflight_scan …")
            code = self._run_python_module(
                cwd=repo_root,
                module="taxtrack.root.preflight_scan",
                args=["--customer-dir", str(customer_dir), "--customer", slug, "--year", str(year)],
            )
            if code != 0:
                self._log(f"[2/2][WARN] preflight_scan Exit-Code {code}")

            report_path = customer_dir / "reports" / f"preflight_scan_{year}.json"
            if report_path.is_file():
                self._log(f"[DONE] Scan report: {report_path}")
                self.after(0, lambda: self._set_phase_status("Preflight fertig"))
                self.after(0, lambda: self.var_status.set(f"Scan report erstellt: {report_path}"))
                # Open button opens folder for scan report
                self._report_pdf_path = report_path
                self.after(0, lambda: self.btn_open.configure(state="normal"))
            else:
                self._log(f"[DONE][WARN] Erwartete Datei fehlt: {report_path}")
                self.after(0, lambda: self._set_phase_status("Preflight fertig"))
                self.after(0, lambda: self.var_status.set(f"Kein Scan report gefunden: {report_path}"))
                self._report_pdf_path = report_path
                self.after(0, lambda: self.btn_open.configure(state="normal"))

        except Exception as e:
            self._log(f"[PREFLIGHT][FEHLER] {e}\n{traceback.format_exc()}")
            self.after(0, lambda: self._set_phase_status("Fehler"))
            self.after(0, lambda: messagebox.showerror("KryptoBilanz", str(e)))
        finally:
            if self.var_wallet_mode.get() == 1 and original_wallets_json is not None:
                try:
                    wallets_json_path.write_text(original_wallets_json, encoding="utf-8")
                except Exception:
                    pass

    def show_frame(self, step: int) -> None:
        if step not in self._step_frames:
            return
        self._step_frames[step].tkraise()
        self._current_step = step
        self._update_step_title()
        if step == STEP_SUMMARY:
            self._refresh_summary()

    def _append_log(self, line: str) -> None:
        self.txt_log.configure(state="normal")
        self.txt_log.insert(END, line + "\n")
        self.txt_log.see(END)
        self.txt_log.configure(state="disabled")

    def _drain_log_queue(self) -> None:
        try:
            while True:
                msg = self._log_queue.get_nowait()
                self._append_log(msg)
        except queue.Empty:
            pass
        self.after(120, self._drain_log_queue)

    def _log(self, line: str) -> None:
        self._log_queue.put(line)

    def _set_phase_status(self, text: str) -> None:
        self.var_process_status.set(text)

    def _add_wallet(self) -> None:
        try:
            raw = self.var_wallet.get().strip()
            if not _valid_wallet(raw):
                messagebox.showwarning("Wallet", "Bitte eine gültige 0x-Adresse eingeben.")
                return
            addr = raw.lower()
            existing = [self.lb_wallets.get(i) for i in range(self.lb_wallets.size())]
            if addr in existing:
                messagebox.showinfo("Wallet", "Adresse ist bereits in der Liste.")
                return
            self.lb_wallets.insert(END, addr)
            self.var_wallet.set("")
        except Exception as e:
            self._log(f"[GUI][ERROR] Wallet hinzufügen: {e}")
            messagebox.showerror("Fehler", str(e))

    def _remove_wallet(self) -> None:
        try:
            sel = self.lb_wallets.curselection()
            if not sel:
                return
            self.lb_wallets.delete(sel[0])
        except Exception as e:
            self._log(f"[GUI][ERROR] Wallet entfernen: {e}")

    def _get_selected_chains(self) -> list[str]:
        out: list[str] = []
        for cid in CHAIN_IDS:
            try:
                if self._chain_vars[cid].get():
                    out.append(cid)
            except Exception:
                continue
        return out

    def _clear_wallet_list_selection(self) -> None:
        try:
            self.lb_wallets.selection_clear(0, END)
        except Exception:
            pass

    def _collect_wallets(self) -> list[str]:
        n = self.lb_wallets.size()
        if n <= 0:
            return []
        sel = self.lb_wallets.curselection()
        if sel:
            return [str(self.lb_wallets.get(i)).strip() for i in sel if str(self.lb_wallets.get(i)).strip()]
        return [str(self.lb_wallets.get(i)).strip() for i in range(n) if str(self.lb_wallets.get(i)).strip()]

    def _on_open_report(self) -> None:
        p = self._report_pdf_path
        if p is None:
            messagebox.showinfo("TaxTrack", "Kein Report-Pfad bekannt.")
            return
        if p.is_file():
            _open_path(p)
        elif p.parent.is_dir():
            _open_path(p.parent)
        else:
            messagebox.showwarning("TaxTrack", "Datei oder Ordner nicht gefunden.")

    def _run_pipeline_safe(
        self,
        display_name: str,
        address: str,
        year: int,
        wallet_addrs: list[str],
        chains: list[str],
    ) -> None:
        repo_root = _repo_root()
        customer_dir = self._selected_customer_dir
        if customer_dir is None:
            self.after(0, lambda: messagebox.showerror("TaxTrack", "Kein Kundenordner ausgewählt."))
            return
        slug = customer_dir.name

        wallet_objs = [{"address": a, "chains": list(chains)} for a in wallet_addrs]
        chains_csv = ",".join(chains)
        wallets_json_path = customer_dir / "wallets.json"
        original_wallets_json: str | None = None

        try:
            self.after(0, lambda: self._set_phase_status("Downloading..."))

            try:
                self._log("[1/4] Erzeuge Kundenordner und wallets.json …")
                # Profile should already exist.
                # For wallet_mode=existing: temporarily override wallets.json to reflect the chosen subset for this run,
                # then restore it after the run to avoid permanent changes.
                if self.var_wallet_mode.get() == 1 and wallets_json_path.exists():
                    try:
                        original_wallets_json = wallets_json_path.read_text(encoding="utf-8")
                    except Exception:
                        original_wallets_json = None
                self._save_wallets_json()
                self._log(f"      OK: {customer_dir}")
            except Exception as e:
                self._log(f"[1/4][FEHLER] {e}\n{traceback.format_exc()}")
                self.after(0, lambda: self._set_phase_status("Fehler"))
                self.after(0, lambda: messagebox.showerror("Schritt 1", str(e)))
                self._write_report_meta(year, wallet_addrs, chains, status="failed", notes=str(e))
                return

            inbox_root = customer_dir / "inbox"

            for i, w in enumerate(wallet_addrs, start=1):
                try:
                    self._log(f"[2/4] Download Wallet {i}/{len(wallet_addrs)}: {w} …")
                    code = self._run_python_module(
                        cwd=repo_root,
                        module="taxtrack.root.download_wallet",
                        args=[
                            "--wallet",
                            w,
                            "--chains",
                            chains_csv,
                            "--inbox",
                            str(inbox_root),
                        ],
                    )
                    if code != 0:
                        self._log(f"[2/4][WARN] download_wallet Exit-Code {code}")
                except Exception as e:
                    self._log(f"[2/4][FEHLER] {e}\n{traceback.format_exc()}")
                    self.after(0, lambda e=e: messagebox.showerror("Download", str(e)))
                    # continue

            self.after(0, lambda: self._set_phase_status("Processing..."))

            try:
                self._log("[3/4] run_customer (Pipeline) …")
                code = self._run_python_module(
                    cwd=repo_root,
                    module="taxtrack.root.run_customer",
                    args=["--customer-dir", str(customer_dir), "--customer", slug, "--year", str(year)],
                )
                if code != 0:
                    self._log(f"[3/4][WARN] run_customer Exit-Code {code}")
                    self.after(
                        0,
                        lambda: messagebox.showwarning(
                            "Pipeline",
                            f"run_customer beendet mit Code {code}. Siehe Log.",
                        ),
                    )
                else:
                    self._log("[3/4] OK")
            except Exception as e:
                self._log(f"[3/4][FEHLER] {e}\n{traceback.format_exc()}")
                self.after(0, lambda: self._set_phase_status("Fehler"))
                self.after(0, lambda: messagebox.showerror("Pipeline", str(e)))
                return

            report_path = customer_dir / "reports" / f"tax_report_{year}.pdf"
            msg = f"Report erstellt unter: {report_path}"

            def finish_ok() -> None:
                self._set_phase_status("PDF erstellt")
                self.var_status.set(msg)
                self._report_pdf_path = report_path
                if report_path.is_file():
                    self.btn_open.configure(state="normal")
                else:
                    self.btn_open.configure(state="normal")

            def finish_missing() -> None:
                hint = f"Erwartete Datei fehlt (evtl. keine Inbox-Daten): {report_path}"
                self._set_phase_status("Abgeschlossen")
                self.var_status.set(hint)
                self._report_pdf_path = report_path
                self.btn_open.configure(state="normal")

            if report_path.is_file():
                self._log(f"[4/4] {msg}")
                self.after(0, finish_ok)
                self._write_report_meta(year, wallet_addrs, chains, status="success", notes="")
            else:
                self._log(f"[4/4] Erwartete Datei fehlt: {report_path}")
                self.after(0, finish_missing)
                self._write_report_meta(
                    year,
                    wallet_addrs,
                    chains,
                    status="failed",
                    notes="pdf_missing",
                )

            # Restore wallets.json if we temporarily overrode it
            if self.var_wallet_mode.get() == 1 and original_wallets_json is not None:
                try:
                    wallets_json_path.write_text(original_wallets_json, encoding="utf-8")
                except Exception:
                    pass

        except Exception as e:
            self._log(f"[UNBEKANNT][FEHLER] {e}\n{traceback.format_exc()}")
            self.after(0, lambda: self._set_phase_status("Fehler"))
            self.after(0, lambda: messagebox.showerror("TaxTrack", str(e)))
            try:
                self._write_report_meta(year, wallet_addrs, chains, status="failed", notes=str(e))
            except Exception:
                pass
            if self.var_wallet_mode.get() == 1 and original_wallets_json is not None:
                try:
                    wallets_json_path.write_text(original_wallets_json, encoding="utf-8")
                except Exception:
                    pass

    # ----------------------------
    # Workspace helpers
    # ----------------------------

    def _refresh_existing_customers(self) -> None:
        try:
            self._customers_root.mkdir(parents=True, exist_ok=True)
            names = sorted([p.name for p in self._customers_root.iterdir() if p.is_dir()])
        except Exception:
            names = []
        try:
            self.cmb_existing_customer.configure(values=names)
        except Exception:
            pass

    def _on_customer_mode_change(self) -> None:
        is_existing = self.var_customer_mode.get() == 1
        try:
            if is_existing:
                self.row_existing_customer.grid()
            else:
                self.row_existing_customer.grid_remove()
        except Exception:
            pass

    def _on_wallet_mode_change(self) -> None:
        # mode affects listbox semantics only; keep UI simple, just refresh if existing
        if self.var_wallet_mode.get() == 1:
            self._load_existing_wallets_into_list()

    def _load_customer_profile(self, customer_dir: Path) -> None:
        info_path = customer_dir / "info.json"
        if not info_path.exists():
            return
        try:
            info = json.loads(info_path.read_text(encoding="utf-8"))
        except Exception:
            return
        if isinstance(info, dict):
            self.var_name.set(str(info.get("name") or ""))
            self.var_company.set(str(info.get("company") or ""))
            try:
                addr = str(info.get("address") or "")
                self.txt_address.delete("1.0", END)
                self.txt_address.insert("1.0", addr)
            except Exception:
                pass
            y = info.get("year")
            if y is not None:
                try:
                    self.var_year.set(str(int(y)))
                except Exception:
                    pass

    def _ensure_customer_profile_saved(self) -> None:
        customer_dir = self._selected_customer_dir
        if customer_dir is None:
            raise ValueError("No customer selected")
        name = (self.var_name.get() or "").strip()
        if not name:
            raise ValueError("Name required")
        try:
            year = int((self.var_year.get() or "").strip())
        except Exception:
            year = datetime.now().year
            self.var_year.set(str(year))
        try:
            address = self.txt_address.get("1.0", END).strip()
        except Exception:
            address = ""
        company = (self.var_company.get() or "").strip() or None

        customers_root = self._customers_root
        customers_root.mkdir(parents=True, exist_ok=True)

        if self.var_customer_mode.get() == 1:
            # Existing: never change folder name/slug implicitly
            customer_dir.mkdir(parents=True, exist_ok=True)
            info = {
                "name": name,
                "address": address,
                "company": company,
                "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "year": int(year),
            }
            (customer_dir / "info.json").write_text(
                json.dumps(info, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            (customer_dir / "inbox").mkdir(exist_ok=True)
            (customer_dir / "reports").mkdir(exist_ok=True)
        else:
            # New: create folder under customers_root
            taxtrack_root = _taxtrack_root()
            created_dir = create_customer_files(
                taxtrack_root,
                display_name=name,
                address=address,
                year=year,
                wallets=[],
                customers_root=customers_root,
                company=company,
            )
            self._selected_customer_dir = created_dir

    def _load_existing_wallets_into_list(self) -> None:
        self.lb_wallets.delete(0, END)
        customer_dir = self._selected_customer_dir
        if customer_dir is None:
            return
        wp = customer_dir / "wallets.json"
        if not wp.exists():
            return
        try:
            data = json.loads(wp.read_text(encoding="utf-8"))
        except Exception:
            return
        wallets = []
        if isinstance(data, dict) and isinstance(data.get("wallets"), list):
            for w in data["wallets"]:
                if isinstance(w, dict) and w.get("address"):
                    wallets.append(str(w["address"]).lower())
        for a in wallets:
            self.lb_wallets.insert(END, a)

    def _save_wallets_json(self) -> None:
        customer_dir = self._selected_customer_dir
        if customer_dir is None:
            raise ValueError("Kein Kundenordner ausgewählt.")

        selected_chains = self._get_selected_chains()
        wallets = self._collect_wallets()
        if not wallets:
            raise ValueError("Keine Wallets ausgewählt.")

        payload = {
            "wallets": [{"address": w.lower(), "chains": list(selected_chains)} for w in wallets]
        }
        (customer_dir / "wallets.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _write_report_meta(
        self,
        year: int,
        wallets: list[str],
        chains: list[str],
        *,
        status: str,
        notes: str,
    ) -> None:
        customer_dir = self._selected_customer_dir
        if customer_dir is None:
            return
        reports_dir = customer_dir / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        pdf_path = reports_dir / f"tax_report_{year}.pdf"
        try:
            address = self.txt_address.get("1.0", END).strip()
        except Exception:
            address = ""
        meta = {
            "customer_slug": customer_dir.name,
            "customer_name": (self.var_name.get() or "").strip(),
            "customer_address": address,
            "company": (self.var_company.get() or "").strip() or None,
            "year": int(year),
            "wallets": [w.lower() for w in wallets],
            "chains": list(chains),
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "pdf_path": str(pdf_path.resolve()),
            "status": status,
            "notes": notes or None,
        }
        (reports_dir / f"report_meta_{year}.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _run_python_module(self, cwd: Path, module: str, args: list[str]) -> int:
        cmd = [sys.executable, "-m", module, *args]
        self._log(f"      $ {' '.join(cmd)}")
        try:
            p = subprocess.Popen(
                cmd,
                cwd=str(cwd),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
        except Exception as e:
            self._log(f"      [subprocess] {e}")
            raise
        assert p.stdout is not None
        for line in p.stdout:
            self._log(f"      {line.rstrip()}")
        return p.wait()


def main() -> None:
    app = TaxTrackLauncher()
    app.mainloop()


if __name__ == "__main__":
    main()
