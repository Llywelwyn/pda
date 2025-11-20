Source config.tape

Output intro.gif
Require echo

Type `pda set welcome \` Sleep 100ms Enter
Type `'{{ if .NIGHT }}Sweet dreams,` Sleep 100ms Enter
Type `{{ else }}Good morning,` Sleep 100ms Enter
Type `{{ end}}{{ default (env "USER") .USER }}'` Sleep 500ms Enter Sleep 1s

Type `pda get welcome` Sleep 500ms Enter Sleep 2s

Type `pda get welcome NIGHT=1 USER=Github` Sleep 500ms Enter Sleep 4s

