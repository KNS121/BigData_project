import json

def read_json_file(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        print(f"Файл {file_path} не найден.")
    except json.JSONDecodeError:
        print(f"Файл {file_path} содержит некорректный JSON.")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
