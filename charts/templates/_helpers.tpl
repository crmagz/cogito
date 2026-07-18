{{- define "cogito.image" -}}
{{- $root := .root -}}
{{- $image := .image -}}
{{- if and $root.Values.global.production (not (regexMatch "^sha256:[a-f0-9]{64}$" $image.digest)) -}}
{{- fail (printf "global.production requires a sha256 digest for image %s" $image.repository) -}}
{{- end -}}
{{- if $image.digest -}}
{{- printf "%s@%s" $image.repository $image.digest -}}
{{- else -}}
{{- printf "%s:%s" $image.repository $image.tag -}}
{{- end -}}
{{- end -}}
