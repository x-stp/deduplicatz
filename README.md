# deduplicatz v1.0.3  
**a quite fast, questionably legal uniq engine powered by `io_uring`, `O_DIRECT`, and `xxHash3`**

> _“once `sort -u`’d a blob while `xz` was running. kernel blinked. I didn’t blink back. ”_

---

## what is this?? explain!

`deduplicatz` is what happens when you take your anger at `sort -u`, wrap it in `O_DIRECT`, bless it with `xxHash3`, and pipeline it through `io_uring` while muttering syscall names like incantations.

It doesn’t sort.  
It doesn’t buffer.  
It doesn’t quite respect traditional memory boundaries.  
It **deduplicates**, fast, loud, and live.

Think:  
- sus' logs. 🍯  
- firmware dumps from your C2 parsers  
- 100 leaked ELFs with CSS version banners  
- `squashfs-root` diffs from a 150-char one-liner that one Chinese vendor site forced you to make  
- the raw unholy output of `strings` on a 2048MB binary blob

It’s like `uniq`, if `uniq` was rewritten in a bunker under the Sea of Japan by someone who drinks strace output for breakfast.

---

## Why not just use `sort -u`?

Because `sort -u` is a trap.  
I ran it once during an `xz -9` session.  
The kernel blinked. I didn’t blink back. 👀
My `.kdbx` evaporated into `/platform/sun4v/kernel/memtrap`.  

That’s when I saw `io_uring` in a dream and woke up sweating `$ man 2|nvim`.  
Kidding — @[kalmjasper](https://github.com/kalmjasper) had to give me a wild airborne intro to `liburing`.  
quickly after, [WCP](https://github.com/wheybags/wcp) got me: coreutils is so legacy.
It’s time for **future build tech**. 🚀🚀

---

## Features

- **100× faster** than coreutils (tested this in a dream)
- **xxHash3** — fast enough to beat collisions in most dimensions
- **`O_DIRECT`** — raw uncut disk reads, no page cache middlemen
- **`io_uring`** — async syscalls that hit different, trust me on that one
- **Batched output via `writev()`** — dump the whole mag
- **64-bit hashset** with dynamic rehashing — it grows, it does
- **Live stats** every 500ms — `[+] Unique: 8347281 | Seen: 9812373`
- **Multithreaded pipeline** — reader, parser, writer, all vibin’
- **No full line buffering** — just hashes. You like RAM? Keep it. Seriously.

---

## Build (or summon) it

```sh
gcc -O3 -march=native -std=gnu11 -Wall -o deduplicatz deduplicatz.c \
    -lpthread -luring -lxxhash
