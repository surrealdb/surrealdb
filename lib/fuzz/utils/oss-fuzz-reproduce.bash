#!/bin/bash

PROJECT_NAME=surrealdb

while [[ $# -gt 0 ]]; do
  case $1 in
    -a)
      architecture="$2"
      shift
      ;;
    -s)
      sanitizer="$2"
      shift
      ;;
    -t)
      target="$2"
      shift
      ;;
    -o)
      ossf="$2"
      shift
      ;;
    -p)
      path="$2"
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [-a <architecture>] [-s <sanitizer>] [-t <target>] [-o <path>] [-p <path>] <case>"
      echo "Options:"
      echo "  -a <architecture>   Architecture to use: x86_64, i386 (default: x86_64)"
      echo "  -s <sanitizer>      Sanitizer to use: address, memory, undefined (default: address)"
      echo "  -t <target>         Fuzz target to use (default: fuzz_structured_executor)"
      echo "  -o <path>           Path to the OSS-Fuzz source (default: .)"
      echo "  -p <path>           If specified, path of the local version of the code"
      echo "  <case>              Path to the test case to reproduce."
      echo "  -h, --help          Display this help message"
      exit 0
      ;;
    *)
      case="$1"
      ;;
  esac
  shift
done

if [ "$case" = false ]; then
  echo "Error: You must specify a test case to reproduce."
  exit 1
fi

architecture="${architecture:-"x86_64"}"
sanitizer="${sanitizer:-"address"}"
target="${target:-"fuzz_structured_executor"}"
ossf="${path:-"."}"
path="${path:-""}"
case="${case:-""}"

cd $ossf
python infra/helper.py build_image $PROJECT_NAME
python infra/helper.py build_fuzzers --sanitizer $sanitizer --architecture $architecture $PROJECT_NAME $path
python infra/helper.py reproduce $PROJECT_NAME $target $case
