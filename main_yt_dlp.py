import json
import re
import os
import sys
import time
import signal
import logging
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, List

# Add project root to path
project_root = os.path.dirname(__file__)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from core.yt_dlp_fetcher import fetch_from_ytdlp
from core.cookie_manager import CookieRotationManager

from huggingface_hub import HfApi, upload_file, upload_large_folder

TOKEN = ""

REPO_ID = "Ilialebedev/yt_dlp"
COOKIE_MANAGER = CookieRotationManager()
SEQUENCE_PATH = os.path.join(project_root, "sequence.json")
TMP_DIR = os.path.join(project_root, "tmp_dir")
os.makedirs(TMP_DIR, exist_ok=True)
PROGRESS_PATH = os.path.join(project_root, "progress.json")
DATA_FILE_SIZE = 500

lcpt = None
lcdt = None

api = HfApi(token=TOKEN)

def video_id_to_url(video_id: str) -> str:
    """Преобразует video_id в YouTube URL."""
    return f"https://www.youtube.com/watch?v={video_id}"

def load_sequence() -> List[str]:
    """
    Загружает sequence.json и возвращает список всех video_id в порядке появления.
    
    Returns:
        Список video_id в порядке появления в sequence.json
    """
    if not os.path.exists(SEQUENCE_PATH):
        print(f"[yt-dlp] sequence.json not found at {SEQUENCE_PATH}")
        return []
    
    try:
        with open(SEQUENCE_PATH, "r", encoding="utf-8") as f:
            sequence = json.load(f)
        
        # Собираем все video_id в порядке появления (по timestamp'ам)
        video_ids = []
        # Сортируем timestamp'ы для правильного порядка
        sorted_timestamps = sorted(sequence.keys())
        for timestamp in sorted_timestamps:
            video_ids.extend(sequence[timestamp])
        
        print(f"[yt-dlp] Loaded {len(video_ids)} video IDs from sequence.json")
        return video_ids
    except Exception as e:
        print(f"[yt-dlp] Error loading sequence.json: {e}")
        return []

def load_progress() -> set[str]:
    """
    Загружает прогресс обработки.
    
    Returns:
        Множество обработанных video_id
    """
    if not os.path.exists(PROGRESS_PATH):
        return set()
    
    try:
        with open(PROGRESS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            processed_ids = data.get("processed_video_ids", [])
            return set(processed_ids) if isinstance(processed_ids, list) else set()
    except Exception as e:
        print(f"[yt-dlp] Error loading progress: {e}")
        return set()

def save_progress(processed_ids: set[str]) -> None:
    """
    Сохраняет прогресс обработки.
    
    Args:
        processed_ids: Множество обработанных video_id
    """
    global lcpt

    try:
        payload = {
            "processed_video_ids": sorted(list(processed_ids)),
            "count": len(processed_ids)
        }
        with open(PROGRESS_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        t = time.time()

        if lcpt:
            if t - lcpt > 150:
                
                upload_file(
                    path_or_fileobj=PROGRESS_PATH,
                    path_in_repo="progress.json",
                    repo_id=REPO_ID,
                    repo_type="dataset"
                )
                lcpt = t

                print(f"[yt-dlp] Файл прогресса выгружен в HF")
                return
        else:
            lcpt = t

        print(f"[yt-dlp] Файл прогресса не выгружен в HF")

    except Exception as e:
        print(f"[yt-dlp] Error saving progress: {e}")

def get_existing_data_files() -> List[str]:
    """
    Возвращает список существующих файлов data_{date}.json.
    
    Returns:
        Список путей к существующим файлам данных
    """
    global api

    data_files = []

    l = api.list_repo_files(
        repo_id=REPO_ID,
        repo_type="dataset",
        token=TOKEN,
    )

    for file in l:
        if file.startswith("data_") and file.endswith(".json"):
            data_files.append(file)
    
    return sorted(data_files)


def get_next_data_file_path() -> str:
    """
    Определяет путь к следующему файлу данных.
    Если последний файл содержит меньше DATA_FILE_SIZE видео, используем его.
    Иначе создаем новый файл с текущей датой.
    
    Returns:
        Путь к файлу данных
    """
    existing_files = get_existing_data_files()
    
    if existing_files:
        # Проверяем последний файл
        last_file = existing_files[-1]
        try:
            import subprocess

            subprocess.run(["wget", "-P", TMP_DIR, f"https://huggingface.co/datasets/{REPO_ID}/resolve/main/{last_file}"])

            p = f"{TMP_DIR}/{last_file}"

            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Считаем количество видео (исключаем служебные ключи)
                video_count = sum(1 for k, v in data.items() if k != "_metadata" and isinstance(v, dict))
                
                if video_count < DATA_FILE_SIZE:
                    # Используем существующий файл
                    return p
                else:
                    upload_file(
                        path_or_fileobj=p,
                        path_in_repo=last_file,
                        repo_id=REPO_ID,
                        repo_type="dataset"
                    )

                    os.remove(p)

                    print(f"[yt-dlp] Файл {last_file} выгружен в hf и удален локально")

        except Exception:
            pass
    
    # Создаем новый файл с текущей датой
    date_str = datetime.now().strftime("%Y-%m-%d")
    # Если файл с такой датой уже существует, добавляем номер
    counter = 1
    while True:
        if counter == 1:
            filename = f"data_{date_str}.json"
        else:
            filename = f"data_{date_str}_{counter}.json"
        
        p = os.path.join(TMP_DIR, filename)
        if not os.path.exists(p):
            return p
        counter += 1


def load_data_file(file_path: str) -> Dict[str, Any]:
    """
    Загружает данные из файла.
    
    Args:
        file_path: Путь к файлу данных
        
    Returns:
        Словарь с данными
    """
    if not os.path.exists(file_path):
        return {"_metadata": {"created_at": datetime.now().isoformat()}}
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[yt-dlp] Error loading data file {file_path}: {e}")
        return {"_metadata": {"created_at": datetime.now().isoformat()}}


def save_data_file(file_path: str, data: Dict[str, Any]) -> None:
    """
    Сохраняет данные в файл.
    
    Args:
        file_path: Путь к файлу данных
        data: Словарь с данными
    """
    try:
        # Обновляем метаданные
        if "_metadata" not in data:
            data["_metadata"] = {}
        data["_metadata"]["updated_at"] = datetime.now().isoformat()
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        t = time.time()

        fname = file_path.split("/")[-1]

        if lcdt:
            if t - lcdt > 150:

                upload_file(
                    path_or_fileobj=file_path,
                    path_in_repo=fname,
                    repo_id=REPO_ID,
                    repo_type="dataset"
                )

                lcdt = t

                print(f"[yt-dlp] Файл {fname} выгружен в hf")

                return
        else:
            lcdt = t

        print(f"[yt-dlp] Файл {fname} не выгружен в hf")

    except Exception as e:
        print(f"[yt-dlp] Error saving data file {file_path}: {e}")


def signal_handler(signum, frame):
    """Обработчик сигнала для корректного завершения."""
    global shutdown_requested
    print(f"\n[yt-dlp] Received signal {signum}. Shutting down gracefully...")
    shutdown_requested = True


def process_videos(
    video_ids_to_process: List[str], 
    processed_ids: set[str], 
    current_data_file: str, 
    current_data: Dict[str, Any]
    ) -> Tuple[set[str], str, Dict[str, Any]]:
    """
    Обрабатывает список видео.
    
    Returns:
        Кортеж (processed_ids, current_data_file, current_data)
    """
    batch_size = 20  
    processed_count = 0
    
    for i, video_id in enumerate(video_ids_to_process):
        # Проверяем флаг завершения

        video_url = video_id_to_url(video_id)
        
        print(f"[yt-dlp] Processing {i+1}/{len(video_ids_to_process)}: {video_id}")
        
        # Получаем данные через yt-dlp
        data = fetch_from_ytdlp(video_url, COOKIE_MANAGER)
        
        if data:
            # Добавляем video_id в данные для удобства
            current_data[video_id] = data
            
            status = "OK"
            if "timings_ytdlp" in data:
                ext_time = data["timings_ytdlp"].get("extract_info_seconds", 0)
                captions_time = data["timings_ytdlp"].get("captions_seconds_total", 0)
                total_time = data["timings_ytdlp"].get("total_seconds", 0)
                
                print(f"[yt-dlp] {status} | {video_url} | ext_time: {ext_time} | captions_time: {captions_time} | total_time: {total_time}")
            else:
                print(f"[yt-dlp] {status} | {video_url}")
        else:
            print(f"[yt-dlp] EMPTY | {video_url}")
        
        # Обновляем прогресс
        processed_ids.add(video_id)
        processed_count += 1
        
        # Сохраняем прогресс и файл данных каждые batch_size видео
        if processed_count % batch_size == 0:
            save_progress(processed_ids)
            print(f"[yt-dlp] Progress saved: {len(processed_ids)} videos processed")
            
            # Сохраняем файл данных
            video_count_in_file = sum(1 for k, v in current_data.items() if k != "_metadata" and isinstance(v, dict))
            save_data_file(current_data_file, current_data)
            print(f"[yt-dlp] Saved data file: {os.path.basename(current_data_file)} ({video_count_in_file} videos)")
        
        # Проверяем, нужно ли перейти к новому файлу (если достигли лимита размера файла)
        video_count_in_file = sum(1 for k, v in current_data.items() if k != "_metadata" and isinstance(v, dict))
        if video_count_in_file >= DATA_FILE_SIZE:
            # Если файл уже был сохранен выше, просто переходим к новому
            if processed_count % batch_size != 0:
                save_data_file(current_data_file, current_data)
                print(f"[yt-dlp] Saved data file (size limit): {os.path.basename(current_data_file)} ({video_count_in_file} videos)")
            
            # Переходим к новому файлу
            current_data_file = get_next_data_file_path()
            current_data = load_data_file(current_data_file)
            print(f"[yt-dlp] Switched to new data file: {os.path.basename(current_data_file)}")
    
    return processed_ids, current_data_file, current_data


def main():
    """Основная функция с динамическим сканированием sequence.json."""
    
    # Загружаем прогресс один раз при старте
    processed_ids = load_progress()
    print(f"[yt-dlp] Already processed: {len(processed_ids)} videos")
    
    # Загружаем текущий файл данных
    current_data_file = get_next_data_file_path()
    current_data = load_data_file(current_data_file)
    print(f"[yt-dlp] Using data file: {os.path.basename(current_data_file)}")
    
    # Проверяем, существует ли sequence.json и изменился ли он
    if os.path.exists(SEQUENCE_PATH):
        # Загружаем sequence.json
        all_video_ids = load_sequence()
        
        if all_video_ids:
            # Фильтруем уже обработанные видео
            video_ids_to_process = [vid for vid in all_video_ids if vid not in processed_ids]
            
            if video_ids_to_process:
                print(f"[yt-dlp] Found {len(video_ids_to_process)} new videos to process")
                
                # Обрабатываем новые видео
                processed_ids, current_data_file, current_data = process_videos(
                    video_ids_to_process, processed_ids, current_data_file, current_data
                )
                
                # Сохраняем финальные результаты после обработки батча
                if current_data:
                    video_count_in_file = sum(1 for k, v in current_data.items() if k != "_metadata" and isinstance(v, dict))
                    if video_count_in_file > 0:
                        save_data_file(current_data_file, current_data)
                        print(f"[yt-dlp] Saved data file: {os.path.basename(current_data_file)} ({video_count_in_file} videos)")
                
                # Финальное сохранение прогресса
                save_progress(processed_ids)
                print(f"[yt-dlp] Progress saved: {len(processed_ids)} videos processed")
            else:
                print(f"[yt-dlp] No new videos to process (total in sequence: {len(all_video_ids)}, processed: {len(processed_ids)})")
        else:
            print(f"[yt-dlp] sequence.json is empty or invalid")
    else:
        print(f"[yt-dlp] Файл sequence.json не существует по пути: {SEQUENCE_PATH}")
        raise


if __name__ == "__main__":
    main()
