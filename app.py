import sys
import os
import time
import random
import csv
import re
import json
import threading
from math import radians, sin, cos, sqrt, atan2

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QTableWidget, QTableWidgetItem, QPushButton, QLabel,
    QLineEdit, QCheckBox, QSpinBox, QComboBox, QTextEdit, QProgressBar,
    QFileDialog, QMessageBox, QHeaderView, QGroupBox, QFrame,
    QScrollArea, QSizePolicy, QSplitter
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer, QSortFilterProxyModel
from PyQt6.QtGui import QFont, QColor, QPalette, QIcon

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}
COLS = ['id', 'price', 'city', 'lat', 'lon', 'bathrooms', 'height',
        'state', 'type', 'year', 'floor', 'max_floor', 'area', 'views']


def prepare_entry(r2, id_):
    result = []
    if r2 is None or r2.status_code != 200:
        return None
    html = r2.text
    soup2 = BeautifulSoup(html, "lxml")

    result.append(str(id_))

    price_el = soup2.select_one("div.offer__price")
    if price_el is None:
        cur_title = soup2.select_one("title")
        if cur_title:
            m = re.search(r"\s[0-9]+\s", cur_title.get_text())
            result.append(m.group(0).strip() if m else None)
        else:
            return None
    else:
        price = price_el.get_text(strip=True).replace("\xa0", "").replace("〒", "")
        result.append(price)

    location_el = soup2.select_one("div.offer__location")
    if location_el is None:
        return None
    city_text = location_el.get_text(strip=True)
    city_match = re.search(r"^[^,]+", city_text)
    result.append(city_match.group() if city_match else city_text)

    lat = re.search(r'"lat"\s*:\s*([0-9.]+)', html)
    lon = re.search(r'"lon"\s*:\s*([0-9.]+)', html)
    if not lat or not lon:
        return None
    result += [lat.group(1), lon.group(1)]

    bathrooms = re.search(r"Санузел(\d)", html)
    if bathrooms:
        result.append(bathrooms.group(1))
    else:
        alt = re.search(r"(\d)\s*с/у", html)
        result.append(alt.group(1) if alt else "1")

    height_m = re.search(r"(\d+\.\d+)\s*м</dd>", html)
    result.append(height_m.group(1) if height_m else "2")

    items = soup2.select("div.offer__info-item")
    text = re.sub(r"\s+", " ", " ".join(i.get_text(" ", strip=True) for i in items).replace("\xa0", " ")).strip()
    state_m = re.search(r"квартиры\s*(.*)", text, re.I)
    result.append(state_m.group(1).strip() if state_m else "черновая отделка")

    type_val, year_val, area_val, floor_val, maxfloor_val = None, None, None, None, None
    for cnt, i in enumerate(soup2.select("div.offer__advert-short-info"), 1):
        t = i.get_text(strip=True)
        if cnt == 2:
            type_val = t
        if m := re.search(r"\b(19|20)\d{2}\b", t):
            year_val = m.group()
        if a := re.search(r"(\d+)\s*м²", t):
            area_val = a.group(1)
        if f := re.search(r"(\d+)\s*из\s*(\d+)", t):
            floor_val, maxfloor_val = f.group(1), f.group(2)

    result += [type_val, year_val, floor_val, maxfloor_val, area_val]

    try:
        r = requests.get(f"https://krisha.kz/ms/views/krisha/live/{id_}/",
                         params={"id": id_}, headers=HEADERS, timeout=10)
        data = r.json()
        nb_p = data["data"][str(id_)]["nb_phone_views"]
        nb_v = data["data"][str(id_)]["nb_views"]
        result.append(nb_p + nb_v)
    except Exception:
        result.append(0)

    if len(result) < len(COLS):
        result += [None] * (len(COLS) - len(result))
    return result[:len(COLS)]


# ─── Worker thread ──────────────────────────────────────────────────────────────

class ScraperWorker(QThread):
    log_signal = pyqtSignal(str, str)       # message, level (ok/skip/err)
    progress_signal = pyqtSignal(int, int)  # done, total
    row_signal = pyqtSignal(list)           # new row data
    finished_signal = pyqtSignal()

    def __init__(self, pages, skip_invalid, auto_fill):
        super().__init__()
        self.pages = pages
        self.skip_invalid = skip_invalid
        self.auto_fill = auto_fill
        self._stop = False

    def stop(self):
        self._stop = True

    def run(self):
        session = requests.Session()
        session.headers.update(HEADERS)
        done = 0
        total_ids = []

        for page in range(1, self.pages + 1):
            if self._stop:
                break
            url = f"https://krisha.kz/prodazha/kvartiry/?page={page}"
            try:
                r = session.get(url, timeout=15)
                soup = BeautifulSoup(r.text, "lxml")
                cards = soup.select("div.a-card")
                for c in cards:
                    iid = c.get("data-id")
                    if iid:
                        total_ids.append(iid)
            except Exception as e:
                self.log_signal.emit(f"Страница {page}: {e}", "err")

        self.progress_signal.emit(0, len(total_ids))

        for iid in total_ids:
            if self._stop:
                break
            time.sleep(1 + random.randint(0, 400) / 1000)
            try:
                r2 = session.get(f"https://krisha.kz/a/show/{iid}", timeout=15)
                result = prepare_entry(r2, iid)
                if result is None:
                    if self.skip_invalid:
                        self.log_signal.emit(f"[ПРОПУСК] id={iid} — невалидная запись", "skip")
                    else:
                        self.log_signal.emit(f"[ОШИБКА] id={iid} — нет данных", "err")
                    done += 1
                    self.progress_signal.emit(done, len(total_ids))
                    continue

                if self.auto_fill:
                    result = [v if v not in (None, "", "None") else "н/д" for v in result]

                self.row_signal.emit(result)
                self.log_signal.emit(f"[OK] id={iid} | {result[2]} | {result[1]} ₸", "ok")
            except Exception as e:
                self.log_signal.emit(f"[ОШИБКА] id={iid}: {e}", "err")

            done += 1
            self.progress_signal.emit(done, len(total_ids))

        self.finished_signal.emit()


# ─── Compare panel ──────────────────────────────────────────────────────────────

class ComparePanel(QWidget):
    ATTRS = [
        ("Цена (₸)", "price"),
        ("Город", "city"),
        ("Площадь (м²)", "area"),
        ("Этаж", "floor"),
        ("Макс. этаж", "max_floor"),
        ("Год", "year"),
        ("Тип", "type"),
        ("Состояние", "state"),
        ("Санузлы", "bathrooms"),
        ("Высота (м)", "height"),
        ("Просмотры", "views"),
        ("Рейтинг", "rating"),
    ]

    def __init__(self):
        super().__init__()
        self.apartments = []
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        top = QHBoxLayout()
        self.clear_btn = QPushButton("Очистить сравнение")
        self.clear_btn.clicked.connect(self.clear)
        top.addWidget(QLabel("Выберите квартиры в таблице (до 4 шт.) и нажмите «Сравнить»"))
        top.addStretch()
        top.addWidget(self.clear_btn)
        layout.addLayout(top)

        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.content = QWidget()
        self.grid_layout = QHBoxLayout(self.content)
        self.grid_layout.setSpacing(12)
        self.scroll.setWidget(self.content)
        layout.addWidget(self.scroll)

    def set_apartments(self, apartments):
        """apartments: list of dicts"""
        self.apartments = apartments
        self._render()

    def clear(self):
        self.apartments = []
        self._render()

    def _render(self):
        # Remove old cards
        while self.grid_layout.count():
            item = self.grid_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        if not self.apartments:
            lbl = QLabel("Нет квартир для сравнения.\nВыберите строки в таблице и нажмите «Сравнить».")
            lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            lbl.setStyleSheet("color: gray; font-size: 14px;")
            self.grid_layout.addWidget(lbl)
            return

        for apt in self.apartments:
            card = self._make_card(apt)
            self.grid_layout.addWidget(card)
        self.grid_layout.addStretch()

    def _make_card(self, apt):
        frame = QFrame()
        frame.setFrameShape(QFrame.Shape.StyledPanel)
        frame.setMinimumWidth(200)
        frame.setMaximumWidth(280)
        layout = QVBoxLayout(frame)

        # Header
        title = QLabel(f"id {apt.get('id', '?')}")
        title.setFont(QFont("", 12, QFont.Weight.Bold))
        layout.addWidget(title)

        city_lbl = QLabel(str(apt.get('city', '')))
        city_lbl.setStyleSheet("color: gray;")
        layout.addWidget(city_lbl)

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        layout.addWidget(sep)

        for label, key in self.ATTRS:
            row = QHBoxLayout()
            k_lbl = QLabel(label)
            k_lbl.setStyleSheet("color: gray; font-size: 12px;")
            v_lbl = QLabel(str(apt.get(key, "—")))
            v_lbl.setFont(QFont("", 11, QFont.Weight.Bold))
            v_lbl.setAlignment(Qt.AlignmentFlag.AlignRight)
            row.addWidget(k_lbl)
            row.addStretch()
            row.addWidget(v_lbl)
            layout.addLayout(row)

        layout.addStretch()
        return frame


# ─── Main window ────────────────────────────────────────────────────────────────

class MainWindow(QMainWindow):
    TABLE_COLS = ['id', 'rating', 'price', 'city', 'area', 'floor',
                  'max_floor', 'year', 'type', 'state', 'bathrooms', 'height', 'views']

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Krisha.kz — Анализатор квартир")
        self.resize(1300, 800)
        self.df = pd.DataFrame()
        self.scraper = None
        self._build_ui()
        self._apply_style()

    # ── UI ──

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(12, 12, 12, 12)

        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)

        self.tabs.addTab(self._build_scrape_tab(), "⟳ Скрейпинг")
        self.tabs.addTab(self._build_data_tab(), "📊 Данные")
        self.tabs.addTab(self._build_compare_tab(), "⇔ Сравнение")

    def _build_scrape_tab(self):
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.setSpacing(12)

        # Options group
        opts = QGroupBox("Параметры")
        opts_lay = QVBoxLayout(opts)

        self.chk_skip = QCheckBox("Пропускать невалидные вхождения (нет цены / координат)")
        self.chk_skip.setChecked(True)
        self.chk_fill = QCheckBox("Автозаполнение пустых полей значением «н/д»")
        self.chk_append = QCheckBox("Дозаписывать в существующий CSV (не перезаписывать)")
        self.chk_append.setChecked(True)

        pages_row = QHBoxLayout()
        pages_row.addWidget(QLabel("Страниц для скрейпинга:"))
        self.pages_spin = QSpinBox()
        self.pages_spin.setRange(1, 50)
        self.pages_spin.setValue(3)
        pages_row.addWidget(self.pages_spin)
        pages_row.addStretch()

        opts_lay.addWidget(self.chk_skip)
        opts_lay.addWidget(self.chk_fill)
        opts_lay.addWidget(self.chk_append)
        opts_lay.addLayout(pages_row)
        layout.addWidget(opts)

        # Buttons
        btn_row = QHBoxLayout()
        self.start_btn = QPushButton("▶  Запустить скрейпинг")
        self.start_btn.setFixedHeight(36)
        self.start_btn.clicked.connect(self.start_scraping)
        self.stop_btn = QPushButton("■  Остановить")
        self.stop_btn.setFixedHeight(36)
        self.stop_btn.setEnabled(False)
        self.stop_btn.clicked.connect(self.stop_scraping)
        btn_row.addWidget(self.start_btn)
        btn_row.addWidget(self.stop_btn)
        btn_row.addStretch()
        layout.addLayout(btn_row)

        # Progress
        self.progress = QProgressBar()
        self.progress.setTextVisible(True)
        self.progress.setFormat("%v / %m объявлений")
        layout.addWidget(self.progress)

        # Log
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setFont(QFont("Courier New", 10))
        self.log_text.setMaximumHeight(300)
        layout.addWidget(self.log_text)

        # Stats row
        stats_row = QHBoxLayout()
        self.lbl_total = QLabel("Собрано: 0")
        self.lbl_skip = QLabel("Пропущено: 0")
        self.lbl_err = QLabel("Ошибок: 0")
        for l in [self.lbl_total, self.lbl_skip, self.lbl_err]:
            stats_row.addWidget(l)
        stats_row.addStretch()
        layout.addLayout(stats_row)

        self._stat_ok = 0
        self._stat_skip = 0
        self._stat_err = 0

        layout.addStretch()
        return w

    def _build_data_tab(self):
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.setSpacing(8)

        # Toolbar
        toolbar = QHBoxLayout()

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Поиск по городу...")
        self.search_edit.textChanged.connect(self.filter_table)
        self.search_edit.setFixedWidth(200)

        self.sort_combo = QComboBox()
        self.sort_combo.addItems([
            "Рейтинг ↓", "Рейтинг ↑", "Цена ↓", "Цена ↑",
            "Площадь ↓", "Площадь ↑", "Просмотры ↓"
        ])
        self.sort_combo.currentIndexChanged.connect(self.sort_table)

        self.city_combo = QComboBox()
        self.city_combo.addItem("Все города")
        self.city_combo.currentIndexChanged.connect(self.filter_table)
        self.city_combo.addItem("Алматы")
        self.city_combo.addItem("Атырау")

        load_btn = QPushButton("📂 Загрузить CSV")
        load_btn.clicked.connect(self.load_csv)
        save_btn = QPushButton("💾 Сохранить CSV")
        save_btn.clicked.connect(self.save_csv)
        run_model_btn = QPushButton("🤖 Рассчитать рейтинг")
        run_model_btn.clicked.connect(self.run_model)

        self.compare_sel_btn = QPushButton("⇔ Сравнить выбранные")
        self.compare_sel_btn.clicked.connect(self.compare_selected)

        for w_ in [QLabel("Поиск:"), self.search_edit,
                   QLabel("Сортировка:"), self.sort_combo,
                   QLabel("Город:"), self.city_combo]:
            toolbar.addWidget(w_)
        toolbar.addStretch()
        for b in [load_btn, save_btn, run_model_btn, self.compare_sel_btn]:
            toolbar.addWidget(b)
        layout.addLayout(toolbar)

        # Table
        self.table = QTableWidget()
        self.table.setColumnCount(len(self.TABLE_COLS))
        self.table.setHorizontalHeaderLabels([
            'ID', 'Рейтинг', 'Цена ₸', 'Город', 'Площадь м²',
            'Этаж', 'Макс. этаж', 'Год', 'Тип', 'Состояние',
            'Санузлы', 'Высота м', 'Просмотры'
        ])
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.setSortingEnabled(True)
        self.table.setAlternatingRowColors(True)
        layout.addWidget(self.table)

        self.row_count_lbl = QLabel("Записей: 0")
        layout.addWidget(self.row_count_lbl)

        return w

    def _build_compare_tab(self):
        self.compare_panel = ComparePanel()
        return self.compare_panel

    def _apply_style(self):
        self.setStyleSheet("""
            QMainWindow { background: #1e1e2e; }
            QWidget { background: #1e1e2e; color: #cdd6f4; font-size: 13px; }
            QTabWidget::pane { border: 1px solid #313244; border-radius: 6px; }
            QTabBar::tab { background: #313244; color: #a6adc8; padding: 8px 18px;
                           border-radius: 4px; margin-right: 3px; }
            QTabBar::tab:selected { background: #89b4fa; color: #1e1e2e; font-weight: bold; }
            QGroupBox { border: 1px solid #45475a; border-radius: 6px; margin-top: 8px;
                        padding-top: 8px; color: #89b4fa; font-weight: bold; }
            QGroupBox::title { subcontrol-origin: margin; left: 10px; }
            QPushButton { background: #313244; color: #cdd6f4; border: 1px solid #45475a;
                          border-radius: 6px; padding: 6px 14px; }
            QPushButton:hover { background: #45475a; }
            QPushButton:pressed { background: #89b4fa; color: #1e1e2e; }
            QPushButton:disabled { color: #585b70; border-color: #313244; }
            QLineEdit, QSpinBox, QComboBox { background: #313244; border: 1px solid #45475a;
                                             border-radius: 4px; padding: 4px 8px; color: #cdd6f4; }
            QTableWidget { background: #181825; alternate-background-color: #1e1e2e;
                           gridline-color: #313244; border: none; }
            QHeaderView::section { background: #313244; color: #89b4fa; padding: 6px;
                                   border: none; font-weight: bold; }
            QTextEdit { background: #11111b; border: 1px solid #313244; border-radius: 4px; }
            QProgressBar { background: #313244; border-radius: 4px; height: 18px;
                           text-align: center; color: #1e1e2e; }
            QProgressBar::chunk { background: #89b4fa; border-radius: 4px; }
            QCheckBox::indicator { width: 16px; height: 16px; border: 1px solid #45475a;
                                   border-radius: 3px; background: #313244; }
            QCheckBox::indicator:checked { background: #89b4fa; }
            QScrollBar:vertical { background: #1e1e2e; width: 10px; }
            QScrollBar::handle:vertical { background: #45475a; border-radius: 5px; }
            QFrame[frameShape="1"] { border: 1px solid #45475a; border-radius: 8px; }
        """)

    # ── Scraping ──

    def start_scraping(self):
        self._stat_ok = self._stat_skip = self._stat_err = 0
        self.log_text.clear()
        self.progress.setValue(0)
        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)

        self.scraper = ScraperWorker(
            pages=self.pages_spin.value(),
            skip_invalid=self.chk_skip.isChecked(),
            auto_fill=self.chk_fill.isChecked()
        )
        self.scraper.log_signal.connect(self.on_log)
        self.scraper.progress_signal.connect(self.on_progress)
        self.scraper.row_signal.connect(self.on_new_row)
        self.scraper.finished_signal.connect(self.on_scrape_done)
        self.scraper.start()

    def stop_scraping(self):
        if self.scraper:
            self.scraper.stop()

    def on_log(self, msg, level):
        colors = {"ok": "#a6e3a1", "skip": "#f9e2af", "err": "#f38ba8"}
        color = colors.get(level, "#cdd6f4")
        self.log_text.append(f'<span style="color:{color}">{msg}</span>')
        if level == "ok":
            self._stat_ok += 1
        elif level == "skip":
            self._stat_skip += 1
        elif level == "err":
            self._stat_err += 1
        self.lbl_total.setText(f"Собрано: {self._stat_ok}")
        self.lbl_skip.setText(f"Пропущено: {self._stat_skip}")
        self.lbl_err.setText(f"Ошибок: {self._stat_err}")

    def on_progress(self, done, total):
        self.progress.setMaximum(total)
        self.progress.setValue(done)

    def on_new_row(self, row_data):
        # row_data matches COLS = ['id','price','city','lat','lon','bathrooms','height','state','type','year','floor','max_floor','area','views']
        row_dict = dict(zip(COLS, row_data))
        new_row = pd.DataFrame([row_dict])
        if self.df.empty:
            self.df = new_row
        else:
            self.df = pd.concat([self.df, new_row], ignore_index=True)
        self._refresh_table()

        # Save incrementally if append mode
        if self.chk_append.isChecked():
            path = "flats.csv"
            write_header = not os.path.isfile(path)
            new_row.to_csv(path, mode='a', header=write_header, index=False)

    def on_scrape_done(self):
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.log_text.append('<span style="color:#89b4fa">── Скрейпинг завершён ──</span>')

    # ── Data table ──

    def _refresh_table(self):
        if self.df.empty:
            self.table.setRowCount(0)
            self.row_count_lbl.setText("Записей: 0")
            return

        # Update city filter
        cities = ["Все города"] + sorted(self.df['city'].dropna().unique().tolist())
        current_city = self.city_combo.currentText()
        self.city_combo.blockSignals(True)
        self.city_combo.clear()
        self.city_combo.addItems(cities)
        if current_city in cities:
            self.city_combo.setCurrentText(current_city)
        self.city_combo.blockSignals(False)

        self._populate_table(self.df)

    def _populate_table(self, df):
        self.table.setSortingEnabled(False)
        self.table.setRowCount(len(df))
        rating_col_map = {
            0: "rating", 1: "rating", 2: "price", 3: "price",
            4: "area", 5: "area", 6: "views"
        }

        for row_i, (_, row) in enumerate(df.iterrows()):
            for col_i, col_name in enumerate(self.TABLE_COLS):
                val = row.get(col_name, "")
                if val is None or (isinstance(val, float) and np.isnan(val)):
                    val = ""
                item = QTableWidgetItem(str(val))

                # Color rating
                if col_name == "rating" and val != "":
                    try:
                        r = int(float(val))
                        if r >= 7:
                            item.setForeground(QColor("#a6e3a1"))
                        elif r >= 4:
                            item.setForeground(QColor("#f9e2af"))
                        else:
                            item.setForeground(QColor("#f38ba8"))
                    except Exception:
                        pass

                self.table.setItem(row_i, col_i, item)

        self.table.setSortingEnabled(True)
        self.row_count_lbl.setText(f"Записей: {len(df)}")

    def filter_table(self):
        if self.df.empty:
            return
        filtered = self.df.copy()
        query = self.search_edit.text().strip().lower()
        if query:
            filtered = filtered[filtered['city'].astype(str).str.lower().str.contains(query, na=False)]
        city = self.city_combo.currentText()
        if city and city != "Все города":
            filtered = filtered[filtered['city'].astype(str) == city]
        self._populate_table(filtered)

    def sort_table(self):
        if self.df.empty:
            return
        idx = self.sort_combo.currentIndex()
        sort_map = {
            0: ("rating", False), 1: ("rating", True),
            2: ("price", False), 3: ("price", True),
            4: ("area", False), 5: ("area", True),
            6: ("views", False)
        }
        col, asc = sort_map.get(idx, ("rating", False))
        if col in self.df.columns:
            self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
            self.df = self.df.sort_values(col, ascending=asc, na_position='last').reset_index(drop=True)
        self._refresh_table()

    # ── File I/O ──

    def load_csv(self):
        path, _ = QFileDialog.getOpenFileName(self, "Открыть CSV", "", "CSV файлы (*.csv)")
        if not path:
            return
        try:
            self.df = pd.read_csv(path)
            self._refresh_table()
            QMessageBox.information(self, "Загружено", f"Загружено {len(self.df)} записей из {path}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))

    def save_csv(self):
        if self.df.empty:
            QMessageBox.warning(self, "Нет данных", "Нет данных для сохранения.")
            return
        path, _ = QFileDialog.getSaveFileName(self, "Сохранить CSV", "flats.csv", "CSV файлы (*.csv)")
        if not path:
            return
        try:
            self.df.to_csv(path, index=False)
            QMessageBox.information(self, "Сохранено", f"Сохранено {len(self.df)} записей в {path}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))

    # ── Model ──

    def run_model(self):
        if self.df.empty:
            QMessageBox.warning(self, "Нет данных", "Сначала загрузите или соберите данные.")
            return
        if not os.path.isfile("apartment_rating_model.json"):
            QMessageBox.warning(self, "Модель не найдена",
                                "Файл apartment_rating_model.json не найден рядом с приложением.")
            return
        if not os.path.isfile("city_centers.csv"):
            QMessageBox.warning(self, "Файл не найден",
                                "city_centers.csv не найден рядом с приложением.")
            return
        try:
            from analysis import clean_scrapped_data, model_predict

            city_centers = pd.read_csv("city_centers.csv")
            city_centers.set_index("city", inplace=True)

            # Очищаем только если есть сырые колонки скрепера (lat/lon/price)
            if 'lat' in self.df.columns and 'lon' in self.df.columns:
                clean_df = clean_scrapped_data(self.df.copy(), city_centers)
            else:
                clean_df = self.df.copy()

            # model_predict заполняет clean_df['rating'] построчно и возвращает df
            result_df = model_predict(clean_df)

            # Синхронизируем rating обратно в self.df по индексу
            self.df.loc[result_df.index, 'rating'] = result_df['rating'].values

            # Сортируем по рейтингу сразу
            self.df['rating'] = pd.to_numeric(self.df['rating'], errors='coerce')
            self.df = self.df.sort_values('rating', ascending=False).reset_index(drop=True)

            # Обновляем таблицу
            self._refresh_table()

            # Перезаписываем flats.csv с новым столбцом rating
            if os.path.isfile("flats.csv"):
                self.df.to_csv("flats.csv", index=False)

            n = result_df['rating'].gt(0).sum()
            QMessageBox.information(self, "Готово",
                f"Рейтинг рассчитан для {n} из {len(result_df)} записей.\n"
                "Таблица отсортирована по рейтингу. flats.csv обновлён.")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка модели", str(e))

    # ── Compare ──

    def compare_selected(self):
        selected_rows = list(set(idx.row() for idx in self.table.selectedIndexes()))
        if not selected_rows:
            QMessageBox.information(self, "Сравнение", "Выберите строки в таблице (до 4 шт.)")
            return
        if len(selected_rows) > 4:
            QMessageBox.warning(self, "Сравнение", "Можно сравнивать не более 4 квартир.")
            return

        apartments = []
        for row in selected_rows:
            apt = {}
            for col_i, col_name in enumerate(self.TABLE_COLS):
                item = self.table.item(row, col_i)
                apt[col_name] = item.text() if item else ""
            apartments.append(apt)

        self.compare_panel.set_apartments(apartments)
        self.tabs.setCurrentIndex(2)


# ─── Entry point ────────────────────────────────────────────────────────────────

def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
