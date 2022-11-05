import argparse
import json

from parser import Parser


arguments_parser = argparse.ArgumentParser(description="Parse GPZU")
arguments_parser.add_argument("-f",
    type=str, required=True, help="Path to PDF file with GPZU")
args = arguments_parser.parse_args()

gpzu_parser = Parser()
gpzu_parser.load_pdf(args.f)

try:
    gpzu_parser.parse()
except Exception as e:
    print("Cannot parse document")
    print(e)

with open("result.json", "w") as f:
    json.dump(gpzu_parser.get_result(), f, ensure_ascii=False, indent="\t")
