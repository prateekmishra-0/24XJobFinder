# Prateek Mishra — Backend Software Engineer
**Location:** Nagpur, Maharashtra
**Phone:** 8595812578
**Email:** mprateek580@gmail.com
**GitHub:** github.com/prateek (see profile)
**LinkedIn:** linkedin.com/in/prateek (see profile)

---

## Education
**Ramdeobaba University (RCOEM)** — B.Tech, CSE with AI/ML
2022–2026 | CGPA: 8.71 | Nagpur, Maharashtra

---

## Internship

### Industrial Trainee — Banking & Financial Services (BFSI)
**Capgemini** | Jan 2026 – April 2026 | Nagpur, Maharashtra

- **Legacy Modernization:** Migrated a role-based portal from raw JDBC/Servlets (HttpSession, RequestDispatcher, JSP) to Spring Boot and Spring Data JPA. Upgraded to Java 11 LTS with Streams, Lambdas, and NIO.2.
- **HATEOAS API Design:** Auto-exposed JPA repositories as HATEOAS HAL+JSON endpoints via Spring Data REST using @Projection and excerptProjection for payload reduction. Secured lifecycles with @RepositoryEventHandler to block upsert exploits and ghost inserts.
- **Hardened API Security:** Deployed a servlet-level Filter at @Order(1) for shared-secret header validation before DispatcherServlet processing. Implemented HandlerInterceptor-based rate limiting via ConcurrentHashMap. Enforced server-side page size caps to neutralize memory bomb attacks.
- **Data Integrity & Concurrency:** Enforced ORM-level soft deletion via @SQLDelete and @SQLRestriction. Implemented ETag-based optimistic locking (409 Conflict) to prevent lost-update anomalies during simultaneous modifications.
- **Quality-Gated CI/CD:** Authored MockMvc and @DataJpaTest pass/fail test suites with @Transactional rollback for zero database side-effects. Configured GitHub Actions to gate all pull requests on full test passage before merging to main.

---

## Projects

### Book Partner Portal — Spring Boot, Spring Data REST, JPA, MySQL, GitHub Actions
Two-module microservices-inspired application built on Microsoft's pubs database schema.

**Backend (Spring Data REST API):**
- Implemented full HATEOAS HAL+JSON API with pagination, sorting, and projections using Spring Data REST — no controllers written
- Soft delete with three coordinated pieces: @SQLDelete (UPDATE instead of DELETE), @SQLRestriction (WHERE is_active=true on all SELECTs), @JsonIgnore (field hidden from API consumers)
- Composite key handling with @EmbeddedId, @Embeddable, custom BackendIdConverter for URL encoding/decoding
- Wired Bean Validation manually into Spring Data REST (non-obvious gotcha — SDR does not run validators automatically)
- ETag-based optimistic locking via MD5 fingerprint to prevent lost-update anomalies
- Filter vs Interceptor vs AOP: implemented custom Servlet-level Filter (@Order(1)) for secret header validation and Spring MVC HandlerInterceptor for rate limiting
- @RepositoryEventHandler for pre-create/pre-save business logic guards

**Frontend (Spring MVC + Thymeleaf SSR):**
- RestClient with RestClientCustomizer for centralized secret header injection across all outbound calls
- Manual HAL+JSON parsing with ObjectMapper and JsonNode traversal (HATEOAS structure does not map to a single Java class cleanly)
- Rate limiting with HandlerInterceptor + ConcurrentHashMap
- Vanilla JS + AJAX + ETag optimistic locking in Employee module

**CI/CD:** GitHub Actions pipeline — MySQL service container in workflow, test reporting (if: always()), manual release via workflow_dispatch

---

### RBAC-Secured Mentorship Ledger & Logging API — Java, Spring Boot, MongoDB
Backend API actively used at RCOEM, eliminating physical check-ins for mentors.

- RBAC with Spring Security, stateless sessions, Basic Auth, BCrypt for authentication and team data isolation
- Atomic multi-document consistency with MongoDB @DBRef and Spring @Transactional
- Concurrent stateless REST APIs with user-context validation via Spring Boot's request-threading model

---

### Real-Time Chat Application — Java, Spring Boot, WebSocket, STOMP
- Real-time messaging using Spring Boot WebSockets over STOMP protocol
- In-memory STOMP broker with persistent WebSocket channels
- Controller-service architecture with strict separation of concerns

---

## Technical Skills

**Languages:** Java (Streams, Lambdas, NIO.2), SQL, HTML, CSS

**Frameworks & Technologies:**
Spring Boot, Spring Data REST, Spring Data JPA, Spring Security (Basic Auth, RBAC, Stateless Sessions — JWT and OAuth2 in progress), Servlets, JSP, JDBC, JUnit 5, MockMvc, WebSocket/STOMP, Hibernate, HATEOAS/HAL+JSON

**Databases:** MySQL, MongoDB

**Infrastructure & Tools:** Docker, AWS (Cloud Practitioner Certified), GitHub Actions (CI/CD), Maven, Postman, Git, Linux (Ubuntu)

**Concepts:** Microservices patterns (BFF, decoupled modules), Soft delete, Composite keys, Optimistic locking, N+1 prevention, Lazy/Eager loading, CSRF, Filter vs Interceptor vs AOP, Front Controller pattern, HATEOAS, Projections, Bean Validation, Constructor injection, Repository event lifecycle

---

## Certifications
- **AWS Certified Cloud Practitioner**

---

## Achievements
- **AINCAT 2026:** AIR 17 in India's Biggest Career Aptitude Test (Naukri Campus) — nationwide
- **HackerRank:** Gold Badge in Java Programming, 5 Stars
