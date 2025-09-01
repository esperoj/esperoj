"""
The 'core' layer: Pure, timeless business rules of the application.

This layer contains the application's domain logic, implemented as pure functions
and business entities (data classes). It is the heart of the system and must not
have any dependencies on external frameworks like Django or any other layer in this project.

Key principles:
- **Framework-Agnostic:** No `import django`. This code should be portable and
  testable without a database or web server.
- **Stateless Functions:** Business logic is primarily implemented as pure,
  stateless functions that operate on primitive data types or simple data structures.
- **Entities for Complexity:** Classes (entities) are used only when necessary to
  manage a cluster of related data and behavior, enforce invariants, or handle
  complex state transitions.
"""
