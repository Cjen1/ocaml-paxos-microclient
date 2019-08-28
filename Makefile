build: microclient.ml
	dune build microclient.exe
	cp _build/default/microclient.exe ./client
