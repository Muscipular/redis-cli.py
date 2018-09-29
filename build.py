import sys
from cx_Freeze import setup, Executable

# Dependencies are automatically detected, but it might need fine tuning.
build_exe_options = {
    "excludes": ["email", 'asyncio', 'pydoc_data', 'unittest'],
    "zip_include_packages": [
        'redis', 'prompt_toolkit', 'encodings', 'collections', 'ctypes',
        'importlib', 'json', 'urllib', 'http', 'wcwidth', 'xml',
        'html', 'logging', 'distutils',
    ],
}

# GUI applications require a different base on Windows (the default is for a
# console application).
base = None
# if sys.platform == "win32":
#     base = "Win32GUI"

setup(name="redis-cli.py",
      version="0.1",
      description="redis cli",
      options={"build_exe": build_exe_options},
      executables=[Executable("redis-cli.py", base=base)])
