#!/bin/env python

# Simple script that just copies all surrealql file in the repository to the corpus

import glob;
import os;
import shutil

SKIP = [
	"surreal-deal-store-mini.surql",
]

def main():
	if not os.path.exists("fuzz_targets"):
		print("Please run this from the parser fuzzing dir")
		return

	if os.path.exists("corpus"):
		print("Corpus directory already exists.")
		return

	os.mkdir("corpus")
	os.mkdir("corpus/fuzz_parser")

	lang_tests = glob.glob("../../../**/*.surql", recursive = True)
	for idx,path in enumerate(lang_tests):
		cont = False
		for s in SKIP:
			if s in path:
				cont = True;
				break
		if cont:
			continue

		shutil.copyfile(path,f"./corpus/fuzz_parser/seed_{idx}")

if __name__ == "__main__":
	main()
