;; calculators.wat - example WebAssembly module for DishtaYantra WASM calculators.
;;
;; Hand-written WebAssembly text (WAT) so it needs no external toolchain to build;
;; wasmtime compiles it directly. In a real project you would more likely compile
;; this from Rust, C, AssemblyScript, or TinyGo to a .wasm file - the host loads
;; either form identically.
;;
;; Each export takes and returns f64 (the scalar boundary used by WasmCalculator
;; v1). Map a record's fields onto the params in order via `input_fields`, and the
;; result lands in `output_field`.
(module
  ;; notional = price * quantity
  ;;   input_fields: ["price", "quantity"]   output_field: "notional"
  (func (export "notional") (param $price f64) (param $qty f64) (result f64)
    local.get $price
    local.get $qty
    f64.mul)

  ;; affine = x * scale + offset   (e.g. unit/currency conversion)
  ;;   input_fields: ["x", "scale", "offset"]   output_field: "y"
  (func (export "affine") (param $x f64) (param $scale f64) (param $offset f64) (result f64)
    local.get $x
    local.get $scale
    f64.mul
    local.get $offset
    f64.add)

  ;; busy_sum = sum 1..n  (intentionally loops; used to demonstrate the `fuel`
  ;; CPU cap - a large n with a small fuel budget traps deterministically)
  (func (export "busy_sum") (param $n f64) (result f64)
    (local $i f64) (local $acc f64)
    (local.set $i (f64.const 1))
    (local.set $acc (f64.const 0))
    (block $done
      (loop $loop
        (br_if $done (f64.gt (local.get $i) (local.get $n)))
        (local.set $acc (f64.add (local.get $acc) (local.get $i)))
        (local.set $i (f64.add (local.get $i) (f64.const 1)))
        (br $loop)))
    local.get $acc))
