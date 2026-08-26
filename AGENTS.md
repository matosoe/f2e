# Ambiente de shell

Neste repositório, execute comandos de shell usando exclusivamente o Git Bash:

`D:\Program Files\Git\bin\bash.exe`

Não use `bash` sem caminho explícito, pois ele resolve para o inicializador do WSL neste Windows. Não use PowerShell ou `cmd` para comandos destinados ao shell do projeto. Quando a execução precisar ser iniciada pelo PowerShell, invoque explicitamente o Git Bash:

```powershell
& 'D:\Program Files\Git\bin\bash.exe' -lc 'comando'
```
