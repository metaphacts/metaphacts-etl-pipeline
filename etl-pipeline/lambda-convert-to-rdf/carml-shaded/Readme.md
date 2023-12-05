# Shaded variant of carml

carml uses `javax.inject.Inject`, but Quarkus 3 has migrated to JakartaEE and 
hence `jakarta.inject.Inject`. To get around package clashes and missing or 
incompatible dependencies, we build our own variant of carml with shaded 
dependencies and a rewritten version using `jakarta.inject.Inject`.
