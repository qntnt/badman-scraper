# Prerequisites
* Python3 + pip3

# Installation
```bash
cd <PATH_TO_BADMAN_SCRAPER>
pip3 install -r requirements.txt
```

# Usage
```bash
cd <PATH_TO_INPUT_XLSX_FILE>
python3 <PATH_TO_BADMAN_SCRAPER> <INPUT_XLSX_FILE>
```

For faster results, increase request concurrency like so

```bash
cd <PATH_TO_INPUT_XLSX_FILE>
python3 <PATH_TO_BADMAN_SCRAPER> -c 20 <INPUT_XLSX_FILE>
```

Use `python3 <PATH_TO_BADMAN_SCRAPER> -h` to see help info.