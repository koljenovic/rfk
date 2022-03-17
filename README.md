# RFKAdapter 0.3.7

RFKAdapter enables easy access to local legacy xBase (DBF) *databases* originally used by dBase, Clipper, FoxPro etc. It includes a native Harbour adapter component for data *table* access and full support for index updating. MEMO fields are skipped, this can be fixed. Indices get sorted alphabetically e.g. `['PROM01', 'PROM02', 'PROM03', 'PROM04']`.
***
RFKAdapter omogućava jednostavan pristup lokalnim legacy xBase (DBF) *bazama podataka* originalno korištenim u programskim jezicima dBase, Clipper, FoxPro etc. Uključuje native Harbour adapter komponentu za pristup *tabelama* i punu podršku za održavanje indeksa. MEMO polja su zanemarena, što može biti popravljeno. Indeksi se sortiraju po alfabetu npr. `['PROM01', 'PROM02', 'PROM03', 'PROM04']`.

# Install

Ensure that `dbfadapter` is compiled and present on the system path, instruction available in `harbour` folder README.
***
Osigurajte da je `dbfadapter` kompajliran i dostupan na `path` putanji pretrživanja, uputstva dostupna u README unutar `harbour` direktorija.

```bash
pip install rfkadapter
```
