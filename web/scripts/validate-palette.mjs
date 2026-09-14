/**
 * Re-run the palette checks that produced the shipped values.
 *
 * The hues in src/palette.ts came out of the dataviz validator, not out
 * of taste — and a future edit would otherwise land with no check at all.
 * This reimplements the checks that matter against the same thresholds,
 * so CI fails if a change breaks them.
 *
 * What it guards, in the order the failures actually happened:
 *   1. chroma floor  — the first attempt used the project accent at
 *      chroma 0.086, which reads as grey once it is a fill
 *   2. CVD separation — that attempt's green/blue pair separated by only
 *      ΔE 5.1 under tritanopia, invisible to a normal-vision reviewer
 *   3. lightness band — dark's band is narrower, so inverting light fails
 */
import {
  CATEGORICAL_DARK,
  CATEGORICAL_LIGHT,
  SEQUENTIAL_DARK,
  SEQUENTIAL_LIGHT,
} from '../src/palette.ts';

const BAND = { light: [0.43, 0.77], dark: [0.48, 0.67] };
const CHROMA_FLOOR = 0.1;
const CVD_FLOOR = 8;
const NORMAL_FLOOR = 15;
const ORDINAL_MIN_DL = 0.06;

const srgbToLinear = (c) =>
  c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;

function rgb(hex) {
  const h = hex.replace('#', '');
  return [0, 2, 4].map((i) => parseInt(h.slice(i, i + 2), 16) / 255);
}

function oklab([r, g, b]) {
  const [R, G, B] = [r, g, b].map(srgbToLinear);
  const l = Math.cbrt(0.4122214708 * R + 0.5363325363 * G + 0.0514459929 * B);
  const m = Math.cbrt(0.2119034982 * R + 0.6806995451 * G + 0.1073969566 * B);
  const s = Math.cbrt(0.0883024619 * R + 0.2817188376 * G + 0.6299787005 * B);
  return [
    0.2104542553 * l + 0.793617785 * m - 0.0040720468 * s,
    1.9779984951 * l - 2.428592205 * m + 0.4505937099 * s,
    0.0259040371 * l + 0.7827717662 * m - 0.808675766 * s,
  ];
}

const lightness = (hex) => oklab(rgb(hex))[0];
const chroma = (hex) => {
  const [, a, b] = oklab(rgb(hex));
  return Math.hypot(a, b);
};

/** Brettel-style simulation, sufficient for a pass/fail gate. */
function simulate(hex, kind) {
  const [r, g, b] = rgb(hex).map(srgbToLinear);
  const m = {
    protan: [[0.1121, 0.8853, -0.0005], [0.1127, 0.8897, -0.0001], [0.0045, 0.0, 1.0]],
    deutan: [[0.292, 0.7054, -0.0003], [0.2934, 0.7089, 0.0], [-0.0197, 0.0247, 1.0]],
    tritan: [[1.0, 0.1193, -0.1193], [0.0, 0.8743, 0.1257], [0.0, 0.1854, 0.8146]],
  }[kind];
  return m.map((row) => row[0] * r + row[1] * g + row[2] * b);
}

const deltaE = (a, b) => {
  const [l1, a1, b1] = a;
  const [l2, a2, b2] = b;
  return Math.hypot(l1 - l2, a1 - a2, b1 - b2) * 100;
};

function oklabLinear([r, g, b]) {
  const l = Math.cbrt(0.4122214708 * r + 0.5363325363 * g + 0.0514459929 * b);
  const m = Math.cbrt(0.2119034982 * r + 0.6806995451 * g + 0.1073969566 * b);
  const s = Math.cbrt(0.0883024619 * r + 0.2817188376 * g + 0.6299787005 * b);
  return [
    0.2104542553 * l + 0.793617785 * m - 0.0040720468 * s,
    1.9779984951 * l - 2.428592205 * m + 0.4505937099 * s,
    0.0259040371 * l + 0.7827717662 * m - 0.808675766 * s,
  ];
}

const failures = [];
const fail = (what) => failures.push(what);

function checkCategorical(name, palette, mode) {
  const [lo, hi] = BAND[mode];
  for (const hex of palette) {
    const L = lightness(hex);
    if (L < lo || L > hi) {
      fail(`${name}: ${hex} L=${L.toFixed(3)} outside ${lo}–${hi}`);
    }
    const C = chroma(hex);
    if (C < CHROMA_FLOOR) {
      fail(`${name}: ${hex} chroma ${C.toFixed(3)} reads as grey`);
    }
  }
  for (let i = 1; i < palette.length; i += 1) {
    const a = palette[i - 1];
    const b = palette[i];
    const normal = deltaE(oklab(rgb(a)), oklab(rgb(b)));
    if (normal < NORMAL_FLOOR) {
      fail(`${name}: ${a}<->${b} normal ΔE ${normal.toFixed(1)} < ${NORMAL_FLOOR}`);
    }
    for (const kind of ['protan', 'deutan', 'tritan']) {
      const d = deltaE(
        oklabLinear(simulate(a, kind)),
        oklabLinear(simulate(b, kind)),
      );
      if (d < CVD_FLOOR) {
        fail(`${name}: ${a}<->${b} ${kind} ΔE ${d.toFixed(1)} < ${CVD_FLOOR}`);
      }
    }
  }
}

function checkSequential(name, palette) {
  const Ls = palette.map(lightness);
  const ascending = Ls.every((l, i) => i === 0 || l >= Ls[i - 1]);
  const descending = Ls.every((l, i) => i === 0 || l <= Ls[i - 1]);
  if (!ascending && !descending) fail(`${name}: lightness is not monotone`);
  for (let i = 1; i < Ls.length; i += 1) {
    const gap = Math.abs(Ls[i] - Ls[i - 1]);
    if (gap < ORDINAL_MIN_DL) {
      fail(`${name}: steps ${i - 1}->${i} ΔL ${gap.toFixed(3)} < ${ORDINAL_MIN_DL}`);
    }
  }
}

checkCategorical('categorical light', CATEGORICAL_LIGHT, 'light');
checkCategorical('categorical dark', CATEGORICAL_DARK, 'dark');
checkSequential('sequential light', SEQUENTIAL_LIGHT);
checkSequential('sequential dark', SEQUENTIAL_DARK);

if (failures.length) {
  console.error('Palette validation FAILED:\n');
  for (const f of failures) console.error(`  - ${f}`);
  console.error(
    '\nSee src/palette.ts for the recorded results these replace.',
  );
  process.exit(1);
}
console.log('Palette validation passed:');
console.log(`  categorical light  ${CATEGORICAL_LIGHT.join(' ')}`);
console.log(`  categorical dark   ${CATEGORICAL_DARK.join(' ')}`);
console.log(`  sequential light   ${SEQUENTIAL_LIGHT.length} steps`);
console.log(`  sequential dark    ${SEQUENTIAL_DARK.length} steps`);
