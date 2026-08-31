#!/usr/bin/env bash

# Quantidade de registros usada pelo fluxo de teste local.
QUANTIDADE_REGISTROS=90

# Se "true", derruba o ambiente (filas, bucket, lambdas) ao final do fluxo.
# Valor padrão: "false" — mantém o ambiente em execução após o término.
DERRUBAR_AMBIENTE=false
