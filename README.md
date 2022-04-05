

# Prerequisites
* [Git](https://git-scm.com/downloads)
* [Python3 + pip3](https://www.python.org/downloads/)

# Installation
If you're using Python3 by default:
```bash
cd <PATH_TO_BADMAN_SCRAPER>
pip install -r requirements.txt
```

If you have Python3 alongside Python2:
```bash
cd <PATH_TO_BADMAN_SCRAPER>
pip3 install -r requirements.txt
```

# Usage
If you're using Python3 by default:
```bash
cd <PATH_TO_INPUT_XLSX_FILE>
python <PATH_TO_BADMAN_SCRAPER> <INPUT_XLSX_FILE>
```

For faster results, increase request concurrency like so

```bash
cd <PATH_TO_INPUT_XLSX_FILE>
python <PATH_TO_BADMAN_SCRAPER> -c 20 <INPUT_XLSX_FILE>
```

Use `python <PATH_TO_BADMAN_SCRAPER> -h` to see help info.

If you have Python3 alongside Python2:
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
