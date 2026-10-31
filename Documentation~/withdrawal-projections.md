# Checked withdrawal projections

Fetch refusals, application refusals, unanswered non-adult declarations and ordinary assignment drops use canonical shared journal work. Fact-purge requests use the same owner and typed forms.

The journal owner uses the spool's workspace, app and environment namespace. Changing a credential or URL retains discovery of that owner's admitted work; each frame keeps its captured assignment scope. Existing-only discovery creates no missing file or directory.

After a write or flush failure, explicit retry reopens the existing journal under the shared writer reservation, preserving the owner and outstanding leases. Checked replay is synchronized before granting projection authority; a complete frame left by a failed flush can finish without repeating projections. Partial or corrupt input remains unchanged and unavailable. Recovery runs outside SDK locks; borrowing an already-open operation performs no I/O.

An assignment observation distinguishes readable absence or a foreign record from unavailable, malformed or oversized input. The latter keeps the decision unfixed. Once fixed, a decision is not observed again.

Assignment projection removes only the named experiment under the exact captured scope at or below its fixed boundary. A foreign scope, a genuinely newer assignment and unrelated history remain intact. Reads are byte-bounded and failed input remains untouched.

Cache identity regression scenes pair independent case-distinct directories with the same canonical cache. They cover in-flight fact withdrawal, projection progress, subject mint gates and asynchronous result fences. A separate rendezvous checks that clearing an unavailable or condemned assignment record cannot delete a valid sibling publication that follows that clear.

Experiment cache fences, emissions, progress, mint fallback and lifecycle matching take their identity from the same confirmed journal owner. Canonical and confirmed case aliases share it; independent case-distinct directories do not. Conservative condemnation keeps its separate folded token. Clearing a record holds the shared storage lock from the availability check through deletion; corrupt-marker cleanup shares that hold before a sibling can publish a new record.

An enabled initialized consumer may establish the ordinary cache root before binding only when the cache-write gate permits it, including granted consent. Closed gates and flag-off consumers create no directory or journal. Passive identity claims create neither. Namespace metadata remains alive alongside process-wide fences without holding journal files open. Only shared state holds the journal Work lease. Pending objects, wrappers, client references and handoff lists borrow without owning a lease. Alias removal or rebinding cannot strand ownership. Active operations pin shared state, including an operation whose invoking alias is concurrently disposed; completion releases the lease only after the final active operation finishes.

Assignment and pipeline operations report their own completion. A false callback leaves its receipt pending; successful callbacks are acknowledged for the exact persisted revision through the shared writer. A later attempt resumes incomplete work without replacing completed receipts.

Checked reads reject malformed entry maps instead of treating omitted entries as absent. Assignment rewrites retain unrelated pending withdrawal intents as well as fact-key history.

The runtime records whole-population age work even when no fact key is known. Located event IDs remain in its frame until actual spool removal succeeds. Active emissions postpone pipeline completion; later admissions persist exemptions for their actual sealed IDs. Flag-off replacements retain destructive work, while replacement cancels constructive assignment writes. Journal I/O runs outside client and consumer locks, and a retained operation outlives client disposal. A memory-only journal supplies same-process coordination without durable storage guarantees.

Handoff fixtures capture each predecessor's own scope and frame ID before replacement. After storage heals and the dark successor runs its persist barrier, a held full journal snapshot observes completed receipts; the incomplete-only discovery view cannot establish completion. Refused writes and an empty journal distinguish memory-only debt from disk rediscovery.

A valid admission blocked by capacity may reclaim complete histories through an immutable, current-state plan. Every unfinished history keeps its full byte charge, canonical latest object, revision, decision and projection receipts; reclaiming completed work never rebases it. A staged write or intervening acknowledgment invalidates the plan. Invalid transitions, oversized frames and a journal containing only unfinished work refuse without eviction.
File reclamation first validates the complete source and retains every revision of unfinished work. It writes and managed-flushes a bounded temporary image, then publishes its prepared name before rebuilding through the still-exclusively-held actual journal handle. The header binds the namespace, exact payload length and whole-payload checksum, including an empty payload.
After rebuilding, managed flush and exact readback precede the model/epoch commit. Prepared evidence is removed before another append. An interrupted rebuild, flush or cleanup remains unavailable until checked recovery finishes; a later acknowledged append can never coexist with an older prepared image. Unpublished temporary input grants no authority. Invalid prepared input remains unchanged with the main journal, and a missing main with prepared evidence is unavailable rather than known absence.
The main journal remains bounded to 1 MiB; each prepared or temporary image is bounded to 1 MiB plus a header of at most 160 bytes. This contract covers managed flush and process interruption, without a directory-fsync or platform power-loss guarantee. Memory storage remains non-persistent.
During a held main-file rebuild flush, the owner keeps both its nonblocking writer reservation and the actual file exclusion. Same-owner writes refuse promptly, client disposal returns while the operation retains ownership, and an independent journal remains writable. After release, the admission completes and replay retains unfinished and new debt. Ownership tests pair a shared read/write handle probe on the actual journal with an independent writable-file control.

Reclaim also advances the owner's admission epoch. An operation holding an older admitted revision-one capture cannot re-admit a completed ID after its history disappears and repeat its projections over newer facts. Weak canonical-frame keys retain capture metadata without a separate completed-ID index or changes to immutable Frame values. Ordinary store replay retains the owner's epoch; completing a prepared reclamation advances it. A fresh initial candidate binds only after byte capacity succeeds, so its own reclaim retry and genuinely new post-reclaim operations remain admissible.
A staged initial append that fails before Write is attempted releases its capture, including offset-read or seek failures. After an attempted write, only a proven zero-byte failure releases the capture. Failures before staging or during reclamation release none, and full or partial uncertain writes retain their fence. This keeps unadmitted debt retryable after unrelated reclamation. Recovering a prepared image advances the retained owner epoch before retiring the image.

Projection authority requires a nonempty scope and an integer `fetched_at_ticks` from zero through one tick below `DateTime.MaxValue`. A missing or malformed timestamp is unknown. A fact key may be absent; when present it must satisfy its grammar.

A checked read must reproduce every member and element through the full serializer without loss. Structural comparison covers entries, history, intents and all nested collections while ignoring whitespace and object-property order. Any discarded or normalized data, including duplicate decoded keys at any depth, keeps the input unchanged and receipts pending.

An authoritative assignment drop skips journal admission only after a checked read establishes that the target is absent. Unknown reads retain work. Journal owners preserve case-distinct paths on case-sensitive platforms.

Case-folded marker tokens remain conservative. Separate case-sensitive paths retain separate journal writers; on Unix, possible aliases reuse a writer only after an empty namespace probe confirms the same location. This probe may run during existing-only discovery, creates no journal or directory, and is removed before sharing.

The temporary-probe exception keeps alias discovery portable on Unity 2022.3, whose BCL has no ResolveLinkTarget API. Any probe or cleanup failure withholds alias authority.

Before either lifecycle peer is bound, the shared handoff predicate compares the enabled predecessor's captured configured path with the successor's normalized configured path and applies the existing tenant-scope rules. Both pre-construction handoff and direct registration use that definition. This non-I/O identity may revoke the old lane; it creates no directory or journal and grants no storage, emission, progress or projection authority. Once either side is bound, matching requires the confirmed owner keys. Independent configured directories and tenants remain independent.

## Shared-state reset contract

`ResetStaticsForTesting` is defined as quiescent global teardown, not a namespace-only reset. All producer, consumer, driver and projection operations must have stopped before it succeeds. It invalidates every borrowed handle, detaches all canonical work and clears process-local fences and identity metadata; disposal runs after the SDK locks are released. Incomplete memory-only work is deliberately discarded by this test-only teardown. It changes no persisted journal bytes.

An active operation makes reset refuse atomically, preserving owners, handles and fences. The paired regression must hold a real projection, prove refusal and continued ownership, then release and join it before proving that quiescent reset releases the final journal reference. The contract was specified before its paired regression.
