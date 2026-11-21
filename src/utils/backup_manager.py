"""Модуль резервного копіювання та відновлення аналітичних вітрин і конфігурацій (п. 11 ТЗ)."""
from __future__ import annotations
import json
import shutil
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from src.utils.config_manager import ConfigManager, PROJECT_ROOT


class BackupManager:
    """Створює архівовані снапшоти шару Gold, метрик ML та конфігурації конвеєра."""

    def __init__(self, config: ConfigManager | None = None) -> None:
        self.config = config or ConfigManager()

    def create_backup(self, tag: str = "scheduled") -> Dict[str, Any]:
        """Формує ZIP-архів із золотими вітринами Parquet, метаданими та конфігурацією."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_name = f"pakapa_gold_backup_{tag}_{timestamp}.zip"
        archive_path = self.config.backup_dir / archive_name

        files_added = 0
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as zf:
            # 1. Додаємо конфігураційний файл
            if self.config.config_path.exists():
                zf.write(
                    self.config.config_path,
                    arcname=f"config/{self.config.config_path.name}",
                )
                files_added += 1

            # 2. Додаємо файли з шару Gold
            if self.config.gold_dir.exists():
                for file_path in self.config.gold_dir.rglob("*"):
                    if file_path.is_file() and not file_path.name.startswith("."):
                        rel_path = file_path.relative_to(PROJECT_ROOT)
                        zf.write(file_path, arcname=str(rel_path))
                        files_added += 1

            # 3. Додаємо маніфест бекапу
            manifest = {
                "backup_name": archive_name,
                "tag": tag,
                "created_at": datetime.now().isoformat(),
                "files_count": files_added,
                "project": "ПАКАПА - Варіант 3",
            }
            zf.writestr("backup_manifest.json", json.dumps(manifest, indent=2, ensure_ascii=False))

        size_kb = round(archive_path.stat().st_size / 1024.0, 2)
        return {
            "backup_file": archive_name,
            "path": str(archive_path),
            "files_count": files_added,
            "size_kb": size_kb,
            "created_at": timestamp,
        }

    def list_backups(self) -> List[Dict[str, Any]]:
        """Повертає список наявних резервних копій у каталозі backups/."""
        result = []
        for f in sorted(self.config.backup_dir.glob("*.zip"), key=lambda x: x.stat().st_mtime, reverse=True):
            result.append(
                {
                    "filename": f.name,
                    "size_kb": round(f.stat().st_size / 1024.0, 2),
                    "modified_at": datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
        return result
