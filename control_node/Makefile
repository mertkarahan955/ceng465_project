PYTHON = venv/bin/python
MONGOSH = mongosh --host mongo-primary.lan --port 27017 --quiet

.PHONY: help install dashboard status clean

help:
	@echo ""
	@echo "  CENG465 — Replication Control Node"
	@echo "  ===================================="
	@echo ""
	@echo "  make install               Install Python dependencies"
	@echo "  make dashboard             Start web dashboard at http://localhost:5001"
	@echo "  make status                Show replica set member states"
	@echo "  make clean                 Drop all fleet + demo collections"
	@echo ""

install:
	python3 -m venv venv
	venv/bin/pip install --quiet -r requirements.txt
	@echo "Dependencies installed."

status:
	$(MONGOSH) --eval "rs.status().members.forEach(m => print(m.name, '-', m.stateStr))"

dashboard: install
	$(PYTHON) app.py

clean: install
	$(PYTHON) -c "\
import db, config; \
pdb = db.get_primary(); \
all_colls = list(config.FLEET_COLLECTIONS) + ['items','operation_logs']; \
[pdb[c].drop() for c in all_colls]; \
print('Collections cleared:', ', '.join(all_colls))"
