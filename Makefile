VENV := .venv
PYTHON := $(VENV)/bin/python3
PIP := $(VENV)/bin/pip3

.PHONY: refresh dashboard test install

$(VENV)/bin/activate:
	python3 -m venv $(VENV)

install: $(VENV)/bin/activate
	$(PIP) install -r requirements.txt

refresh: $(VENV)/bin/activate
	$(PYTHON) scripts/refresh_portfolio.py

dashboard: $(VENV)/bin/activate
	$(PYTHON) scripts/dashboard_server.py --host 127.0.0.1 --port 8787

test:
	python3 -m unittest discover -s tests -p "test_*.py" -v
