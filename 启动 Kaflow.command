#!/bin/zsh
cd "${0:A:h}"
if [[ ! -x ./dist/kaflow ]]; then
  print '尚未构建，请先运行：sh scripts/build-macos.sh'
  read '?按回车退出'
  exit 1
fi
./dist/kaflow
if [[ $? -ne 0 ]]; then
  print '启动失败：若 17893 端口已占用，请关闭已运行的 Kaflow，或运行 ./dist/kaflow -addr 127.0.0.1:17894'
  read '?按回车退出'
fi
