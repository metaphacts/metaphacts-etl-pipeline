# Shaded variant of jsonsurfer

jsonsurfer use `antlr` in version 4.7.2 to parse JSONPath expressions.
Quarkus also uses `antlr`, but in a newer, incompatible version. To get
around version clashes we build our own variant of jsonsurfer with
shaded dependencies and a repackaged version of `antlr`.
