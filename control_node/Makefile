PYTHON = venv/bin/python
MONGOSH = mongosh --host mongo-primary.lan --port 27017 --quiet
OPS ?= 1000
RUN_ID ?= run1

.PHONY: help install demo test trace trace-secondary benchmark benchmark-writer benchmark-reader clean status chart

help:
	@echo ""
	@echo "  CENG465 — Replication Control Node"
	@echo "  ===================================="
	@echo ""
	@echo "  make install               Install Python dependencies"
	@echo "  make demo                  Run visualization demo (insert/update/delete + chart)"
	@echo "  make test                  Run full test suite (13 assertions)"
	@echo "  make trace                 Live trace from PRIMARY  (separate terminal)"
	@echo "  make trace-secondary       Live trace from SECONDARY (other machine)"
	@echo ""
	@echo "  make dashboard             Start web dashboard at http://localhost:5001"
	@echo "  make benchmark             Single-machine benchmark (OPS=1000)"
	@echo "  make benchmark-writer      Two-machine writer side  (RUN_ID=run1 OPS=1000)"
	@echo "  make benchmark-reader      Two-machine reader side  (RUN_ID=run1) — other machine"
	@echo ""
	@echo "  make status                Show replica set member states"
	@echo "  make chart                 Regenerate chart from existing operation_logs"
	@echo "  make clean                 Drop all benchmark + demo collections"
	@echo ""

install:
	python3 -m venv venv
	venv/bin/pip install --quiet -r requirements.txt
	@echo "Dependencies installed."

demo: install
	$(PYTHON) visualize.py

test: install
	$(PYTHON) test_replication.py

trace: install
	$(PYTHON) trace.py

trace-secondary: install
	$(PYTHON) trace.py --secondary

status:
	$(MONGOSH) --eval "rs.status().members.forEach(m => print(m.name, '-', m.stateStr))"

chart: install
	$(PYTHON) -c "\
import matplotlib; matplotlib.use('Agg'); \
from visualize import save_chart; \
import db; \
logs = list(db.get_primary()['operation_logs'].find().sort('log_index', 1)); \
print('Chart saved:', save_chart(logs)) if logs else print('No logs found.')"

dashboard: install
	$(PYTHON) app.py

benchmark: install
	$(PYTHON) benchmark.py --mode single --ops $(OPS)

benchmark-writer: install
	$(PYTHON) benchmark.py --mode writer --ops $(OPS) --run-id $(RUN_ID)

benchmark-reader: install
	$(PYTHON) benchmark.py --mode reader --run-id $(RUN_ID)

clean: install
	$(PYTHON) -c "\
import db; \
pdb = db.get_primary(); \
[pdb[c].drop() for c in ['items','operation_logs','benchmark_results','benchmark_manifest']]; \
print('Collections cleared.')"
