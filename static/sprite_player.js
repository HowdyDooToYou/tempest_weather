(function () {
  const FRAME_SIZE = 128;
  const SHEET_COLS = 8;

  const sequences = {
    flagCalm: [0], // top-left calm flag
    flagBreeze: [8], // looser flag
    flagStorm: [9], // lightning/skull flag
    skeletonIdle: [16, 17], // slow wave
    skeletonDance: [18, 19, 20, 21], // dancing / gesturing
    skeletonLightning: [26, 27, 28], // lightning frames
    lightningIcon: [11], // big bolt
  };

  function frameRectForSheet(sheet, frameIndex, frameSize) {
    const size = frameSize || FRAME_SIZE;
    if (sheet && Array.isArray(sheet.frameRects) && sheet.frameRects[frameIndex]) {
      const rect = sheet.frameRects[frameIndex];
      return { sx: rect.sx, sy: rect.sy, w: rect.w, h: rect.h };
    }
    if (!sheet || sheet.type === "strip" || sheet.type === "single") {
      return { sx: frameIndex * size, sy: 0, w: size, h: size };
    }
    const cols = sheet.columns || SHEET_COLS;
    const row = Math.floor(frameIndex / cols);
    const col = frameIndex % cols;
    return { sx: col * size, sy: row * size, w: size, h: size };
  }

  function buildFrameIndex(manifest) {
    if (!manifest || !manifest.sheets) {
      return { frameSize: FRAME_SIZE, sheets: {}, framesByName: {} };
    }
    const baseSize = manifest.frame_size || FRAME_SIZE;
    const index = { frameSize: baseSize, sheets: {}, framesByName: {} };
    manifest.sheets.forEach((sheet) => {
      const name = sheet.name || sheet.path || "unknown";
      const entry = {
        name,
        path: sheet.path,
        type: sheet.type || "strip",
        columns: sheet.columns || SHEET_COLS,
        frameSize: sheet.frame_size || baseSize,
        frames: sheet.frames || (sheet.frame_names ? sheet.frame_names.length : 0),
        frameRects: sheet.frame_rects || null,
      };
      index.sheets[name] = entry;
      const frameNames = sheet.frame_names || [];
      frameNames.forEach((frameName, i) => {
        const rect = frameRectForSheet(entry, i, entry.frameSize);
        index.framesByName[frameName] = { ...rect, sheet: name, index: i };
      });
    });
    return index;
  }

  function resolveFrame(manifest, frameName) {
    if (!manifest || !frameName) return null;
    const index = buildFrameIndex(manifest);
    return index.framesByName[frameName] || null;
  }

  function loadSpriteManifest(url = "static/sprite_manifest.json") {
    return fetch(url).then((res) => {
      if (!res.ok) {
        throw new Error(`Failed to load sprite manifest (${res.status})`);
      }
      return res.json();
    });
  }

  function drawFrameByName(ctx, imagesBySheet, manifest, frameName, dx, dy, scale = 1, alpha = 1) {
    if (!ctx || !imagesBySheet || !manifest || !frameName) return false;
    const frame = resolveFrame(manifest, frameName);
    if (!frame) return false;
    const img = imagesBySheet[frame.sheet];
    if (!img || !img.complete) return false;
    const w = frame.w * scale;
    const h = frame.h * scale;
    ctx.save();
    ctx.globalAlpha = alpha;
    ctx.drawImage(img, frame.sx, frame.sy, frame.w, frame.h, dx, dy, w, h);
    ctx.restore();
    return true;
  }

  function loadSheetImages(manifest, { baseUrl = "" } = {}) {
    if (!manifest || !manifest.sheets) {
      return Promise.resolve({ imagesBySheet: {}, errors: [] });
    }
    const imagesBySheet = {};
    const errors = [];
    const normalizePath = (p) => (p || "").replace(/\\/g, "/");
    const loadOne = (sheet) =>
      new Promise((resolve) => {
        const path = normalizePath(sheet.path || "");
        if (!path) {
          errors.push({ sheet: sheet.name || "unknown", error: "Missing path" });
          resolve();
          return;
        }
        const img = new Image();
        img.onload = () => {
          imagesBySheet[sheet.name] = img;
          resolve();
        };
        img.onerror = () => {
          errors.push({ sheet: sheet.name || path, error: "Failed to load image" });
          resolve();
        };
        img.src = `${baseUrl}${path}`;
      });
    const tasks = manifest.sheets.map(loadOne);
    return Promise.all(tasks).then(() => ({ imagesBySheet, errors }));
  }

  function frameToXY(frame) {
    const row = Math.floor(frame / SHEET_COLS);
    const col = frame % SHEET_COLS;
    return { sx: col * FRAME_SIZE, sy: row * FRAME_SIZE };
  }

  function buildKeyedImage(img, tolerance = 12) {
    const off = document.createElement("canvas");
    off.width = img.width;
    off.height = img.height;
    const ctx = off.getContext("2d");
    ctx.drawImage(img, 0, 0);
    const imgData = ctx.getImageData(0, 0, off.width, off.height);
    const data = imgData.data;
    const key = [data[0], data[1], data[2]]; // sample top-left as the matte color
    for (let i = 0; i < data.length; i += 4) {
      const dr = Math.abs(data[i] - key[0]);
      const dg = Math.abs(data[i + 1] - key[1]);
      const db = Math.abs(data[i + 2] - key[2]);
      if (dr < tolerance && dg < tolerance && db < tolerance) {
        data[i + 3] = 0; // knock out background to transparent
      }
    }
    ctx.putImageData(imgData, 0, 0);
    return off;
  }

  function loadCharacterStrips(strips, label) {
    if (!strips) return null;
    const out = {};
    Object.entries(strips).forEach(([key, info]) => {
      if (!info || !info.uri) return;
      const img = new Image();
      const entry = { img, frames: info.frames || 1, ready: false };
      img.onload = () => {
        entry.ready = true;
      };
      img.onerror = () => {
        console.warn("Character strip failed to load", label, key);
      };
      img.src = info.uri;
      out[key] = entry;
    });
    return out;
  }

  function loadCharacterSets(data) {
    if (!data) return [];
    return Object.entries(data).map(([name, payload]) => ({
      name,
      strips: loadCharacterStrips(payload.strips || payload, name),
      effect: payload.effect || "none",
      projectileUri: payload.projectileUri || null,
      sequences: payload.sequences || {},
    }));
  }

  class CharacterAnimator {
    constructor(canvas, ctx, config) {
      this.canvas = canvas;
      this.ctx = ctx;
      this.strips = (config && config.strips) || {};
      this.effect = (config && config.effect) || "none";
      this.projectileUri = (config && config.projectileUri) || null;
      this.sequences = (config && config.sequences) || {};
      this.scale = 0.935;
      this.state = {
        seq: "idle",
        idx: 0,
        timer: 0,
      };
      this.wind = { mph: 0, dir: 90 };
      this.throwTimer = 0;
      this.projectiles = [];
      this.slashes = [];
      this.anchor = { x: canvas.width * 0.5, y: canvas.height * 0.78 };
      this.walkerX = this.canvas.width + FRAME_SIZE * this.scale;
      this.walkSpeed = 22;
      this.walkDir = -1;
      this.spearImage = null;
      this.spearReady = false;
      this.arrowImage = null;
      this.arrowReady = false;
      this.popTimer = 0;
      this.popOffset = 0;
    }

    triggerPop() {
      this.popTimer = 1.4;
    }

    updatePayload({ windMph = 0, windDirDeg = 90 } = {}) {
      this.wind.mph = windMph;
      this.wind.dir = Number.isFinite(windDirDeg) ? windDirDeg : 90;
      const angle = ((this.wind.dir || 0) - 90) * (Math.PI / 180);
      this.walkDir = Math.cos(angle) >= 0 ? 1 : -1;
      const base = this.wind.mph >= 18 ? 88 : this.wind.mph >= 10 ? 70 : this.wind.mph >= 4 ? 52 : 38;
      this.walkSpeed = base;
    }

    pickSequence() {
      const runAttack = this.sequences.runAttack || "runAttack";
      const run = this.sequences.run || "run";
      const walk = this.sequences.walk || "walk";
      const idle = this.sequences.idle || "idle";
      if (this.wind.mph >= 18) return runAttack;
      if (this.wind.mph >= 10) return run;
      if (this.wind.mph >= 4) return walk;
      return idle;
    }

    advanceSeq(dt) {
      const seqName = this.pickSequence();
      const strip = this.strips[seqName];
      const frames = strip ? strip.frames : 1;
      const windBoost = 1 + Math.min(2.2, this.wind.mph / 10);
      const speed =
        seqName === "idle"
          ? 0.38 / windBoost
          : seqName === "walk"
          ? 0.12 / windBoost
          : seqName === "run"
          ? 0.09 / windBoost
          : 0.08 / windBoost;
      this.state.seq = seqName;
      this.state.timer += dt;
      if (this.state.timer >= speed) {
        this.state.timer = 0;
        this.state.idx = (this.state.idx + 1) % frames;
      }
    }

    maybeThrow(dt) {
      const speedTier =
        this.wind.mph >= 18
          ? "fast"
          : this.wind.mph >= 10
          ? "medium"
          : this.wind.mph >= 6
          ? "slow"
          : null;
      if (!speedTier) {
        this.throwTimer = 0.4;
        return;
      }
      this.throwTimer -= dt;
      const cadence = speedTier === "fast" ? 0.9 : speedTier === "medium" ? 1.4 : 2.4;
      if (this.throwTimer <= 0) {
        this.throwTimer = cadence;
        if (this.effect === "spear" || this.effect === "arrow") {
          this.spawnProjectile(speedTier);
        } else if (this.effect === "slash") {
          this.spawnSlash(speedTier);
        }
        const attackSeq = this.sequences.attack || "attack";
        const runAttackSeq = this.sequences.runAttack || "runAttack";
        this.state.seq = speedTier === "fast" ? runAttackSeq : attackSeq;
      }
    }

    spawnProjectile(speedTier) {
      const img = this.effect === "arrow" ? this.arrowImage : this.spearImage;
      if (!img) return;
      const angle = ((this.wind.dir || 0) - 90) * (Math.PI / 180);
      const base =
        speedTier === "fast" ? 240 : speedTier === "medium" ? 170 : 120;
      const speed = base + this.wind.mph * 4.2;
      const spriteW = FRAME_SIZE * this.scale;
      const offsetX = this.walkDir < 0 ? spriteW * 0.25 : spriteW * 0.7;
      const offsetY = FRAME_SIZE * this.scale * 0.55;
      this.projectiles.push({
        x: this.walkerX + offsetX,
        y: this.anchor.y - offsetY,
        vx: Math.cos(angle) * speed,
        vy: Math.sin(angle) * speed,
        life: 2.8,
        image: img,
      });
    }

    updateProjectiles(dt) {
      this.projectiles.forEach((s) => {
        s.x += s.vx * dt;
        s.y += s.vy * dt;
        s.life -= dt;
      });
      this.projectiles = this.projectiles.filter(
        (s) =>
          s.life > 0 &&
          s.x > -80 &&
          s.x < this.canvas.width + 80 &&
          s.y > -80 &&
          s.y < this.canvas.height + 80
      );
    }

    drawProjectiles() {
      this.ctx.save();
      this.projectiles.forEach((s) => {
        const img = s.image;
        if (!img) return;
        const halfW = img.width / 2;
        const halfH = img.height / 2;
        const ang = Math.atan2(s.vy, s.vx);
        this.ctx.translate(s.x, s.y);
        this.ctx.rotate(ang);
        this.ctx.drawImage(img, -halfW, -halfH, img.width, img.height);
        this.ctx.setTransform(1, 0, 0, 1, 0, 0);
      });
      this.ctx.restore();
    }

    spawnSlash(speedTier) {
      const strength = speedTier === "fast" ? 1.2 : speedTier === "medium" ? 1 : 0.85;
      this.slashes.push({
        x: this.walkerX + FRAME_SIZE * this.scale * 0.6,
        y: this.anchor.y - FRAME_SIZE * this.scale * 0.5,
        life: 0.35,
        strength,
      });
    }

    drawSlashes(dt) {
      if (!this.slashes.length) return;
      this.ctx.save();
      this.ctx.strokeStyle = "rgba(255,226,153,0.8)";
      this.ctx.lineWidth = 2;
      this.slashes = this.slashes.filter((s) => s.life > 0);
      this.slashes.forEach((s) => {
        s.life -= dt;
        const radius = 16 * s.strength;
        this.ctx.beginPath();
        this.ctx.arc(s.x, s.y, radius, Math.PI * 0.1, Math.PI * 0.7);
        this.ctx.stroke();
      });
      this.ctx.restore();
    }

    ensureSpearSprite() {
      if (this.spearReady) return;
      const attack = this.strips[this.sequences.attack || "attack"];
      if (!attack || !attack.img || !attack.ready) return;
      const keyed = buildKeyedImage(attack.img);
      const frames = attack.frames || 1;
      const frameIndex = Math.max(0, frames - 1);
      const frameX = frameIndex * FRAME_SIZE;
      const frameY = 0;
      const ctx = keyed.getContext("2d");
      if (!ctx) return;
      const imgData = ctx.getImageData(frameX, frameY, FRAME_SIZE, FRAME_SIZE);
      const data = imgData.data;
      let minX = FRAME_SIZE, minY = FRAME_SIZE, maxX = 0, maxY = 0;
      const cutX = Math.floor(FRAME_SIZE * 0.55);
      for (let y = 0; y < FRAME_SIZE; y++) {
        for (let x = cutX; x < FRAME_SIZE; x++) {
          const idx = (y * FRAME_SIZE + x) * 4 + 3;
          if (data[idx] > 20) {
            if (x < minX) minX = x;
            if (y < minY) minY = y;
            if (x > maxX) maxX = x;
            if (y > maxY) maxY = y;
          }
        }
      }
      if (minX >= maxX || minY >= maxY) {
        this.spearReady = true;
        return;
      }
      const w = maxX - minX + 1;
      const h = maxY - minY + 1;
      const spearCanvas = document.createElement("canvas");
      spearCanvas.width = w;
      spearCanvas.height = h;
      const spearCtx = spearCanvas.getContext("2d");
      if (!spearCtx) return;
      spearCtx.drawImage(
        keyed,
        frameX + minX,
        frameY + minY,
        w,
        h,
        0,
        0,
        w,
        h
      );
      this.spearImage = spearCanvas;
      this.spearReady = true;
    }

    ensureArrowSprite() {
      if (this.arrowReady) return;
      if (!this.projectileUri) {
        this.arrowReady = true;
        return;
      }
      const img = new Image();
      img.onload = () => {
        this.arrowImage = img;
        this.arrowReady = true;
      };
      img.onerror = () => {
        this.arrowReady = true;
      };
      img.src = this.projectileUri;
    }

    drawSprite() {
      const strip = this.strips[this.state.seq] || this.strips.idle;
      if (!strip || !strip.img || !strip.img.complete || !strip.ready) return;
      const frames = strip.frames || 1;
      const idx = Math.min(this.state.idx, frames - 1);
      const sx = idx * FRAME_SIZE;
      const sy = 0;
      const scale = this.scale;
      const dx = this.walkerX;
      const dy = this.anchor.y + this.popOffset - FRAME_SIZE * scale;
      const drawW = FRAME_SIZE * scale;
      const drawH = FRAME_SIZE * scale;
      this.ctx.save();
      if (this.walkDir < 0) {
        this.ctx.translate(dx + drawW, 0);
        this.ctx.scale(-1, 1);
        this.ctx.drawImage(strip.img, sx, sy, FRAME_SIZE, FRAME_SIZE, 0, dy, drawW, drawH);
      } else {
        this.ctx.drawImage(strip.img, sx, sy, FRAME_SIZE, FRAME_SIZE, dx, dy, drawW, drawH);
      }
      this.ctx.restore();
    }

    draw(dt) {
      if (this.effect === "spear") {
        this.ensureSpearSprite();
      }
      if (this.effect === "arrow") {
        this.ensureArrowSprite();
      }
      if (this.popTimer > 0) {
        this.popTimer = Math.max(0, this.popTimer - dt);
        const t = 1 - this.popTimer / 1.4;
        this.popOffset = -Math.sin(t * Math.PI) * 12;
      } else {
        this.popOffset = 0;
      }
      this.walkerX += this.walkSpeed * dt * this.walkDir;
      if (this.walkDir < 0 && this.walkerX < -FRAME_SIZE * this.scale * 1.2) {
        this.walkerX = this.canvas.width + FRAME_SIZE * this.scale * 0.2;
      } else if (this.walkDir > 0 && this.walkerX > this.canvas.width + FRAME_SIZE * this.scale * 0.2) {
        this.walkerX = -FRAME_SIZE * this.scale * 1.2;
      }
      this.advanceSeq(dt);
      this.maybeThrow(dt);
      this.updateProjectiles(dt);
      this.drawProjectiles();
      this.drawSlashes(dt);
      this.drawSprite();
    }
  }

  class SpriteSheetPlayer {
    constructor(canvas, img, spearmanData) {
      this.canvas = canvas;
      this.ctx = canvas.getContext("2d");
      this.img = img ? buildKeyedImage(img) : null;
      this.t = 0;
      this.last = performance.now();
      this.lastPayload = {
        windMph: 0,
        windDirDeg: 90,
        lightningCount: 0,
        lightningNear: false,
        ingestRate: 0,
      };
      this.state = {
        flagSeq: sequences.flagCalm,
        flagIdx: 0,
        flagTimer: 0,
        skelSeq: sequences.skeletonIdle,
        skelIdx: 0,
        skelTimer: 0,
        boltSeq: [],
        boltIdx: 0,
        boltTimer: 0,
      };
      this.lightning = {
        active: false,
        timer: 0,
        bolts: [],
      };
      this.characters = loadCharacterSets(spearmanData);
      this.charIndex = 0;
      this.character = this.characters.length
        ? new CharacterAnimator(canvas, this.ctx, this.characters[this.charIndex])
        : null;
      this.debugAlive = true;
      requestAnimationFrame(this.loop);
    }

    updatePayload({ windMph, lightningCount, ingestRate, windDirDeg, lightningNear }) {
      this.lastPayload = {
        windMph,
        windDirDeg,
        lightningCount,
        lightningNear,
        ingestRate,
      };
      const flagSeq =
        lightningCount > 0
          ? sequences.flagStorm
          : windMph >= 14
          ? sequences.flagStorm
          : windMph >= 7
          ? sequences.flagBreeze
          : sequences.flagCalm;

      const skelSeq =
        lightningCount > 0
          ? sequences.skeletonLightning
          : ingestRate > 0
          ? sequences.skeletonDance
          : sequences.skeletonIdle;

      this.state.flagSeq = flagSeq;
      this.state.skelSeq = skelSeq;
      this.state.boltSeq = lightningCount > 0 ? sequences.lightningIcon : [];
      this.lightning.active = Boolean(lightningNear || lightningCount > 0);
      if (this.character) {
        this.character.updatePayload({ windMph, windDirDeg });
      }
    }

    toggleCharacter() {
      if (!this.characters.length) return;
      this.charIndex = (this.charIndex + 1) % this.characters.length;
      this.character = new CharacterAnimator(this.canvas, this.ctx, this.characters[this.charIndex]);
      this.character.updatePayload({
        windMph: this.lastPayload.windMph,
        windDirDeg: this.lastPayload.windDirDeg,
      });
    }

    triggerEasterEgg() {
      if (this.character && this.character.triggerPop) {
        this.character.triggerPop();
      }
    }

    loop = (now) => {
      const dt = Math.min(0.05, (now - this.last) / 1000);
      this.last = now;
      this.draw(dt);
      requestAnimationFrame(this.loop);
    };

    updateLightning(dt) {
      if (!this.lightning.active) {
        this.lightning.bolts = [];
        return;
      }
      this.lightning.timer -= dt;
      if (this.lightning.timer <= 0) {
        const startX = Math.random() * this.canvas.width;
        const startY = -8;
        const segments = 4 + Math.floor(Math.random() * 3);
        const points = [{ x: startX, y: startY }];
        for (let i = 0; i < segments; i++) {
          const last = points[points.length - 1];
          points.push({
            x: last.x + (Math.random() * 14 - 7),
            y: last.y + 18 + Math.random() * 12,
          });
        }
        this.lightning.bolts.push({ points, life: 0.22 });
        this.lightning.timer = 0.35 + Math.random() * 0.4;
      }
      this.lightning.bolts.forEach((bolt) => {
        bolt.life -= dt;
      });
      this.lightning.bolts = this.lightning.bolts.filter((b) => b.life > 0);
    }

    drawLightning() {
      if (!this.lightning.bolts.length) return;
      this.ctx.save();
      this.ctx.strokeStyle = "rgba(255,241,187,0.9)";
      this.ctx.lineWidth = 2;
      this.lightning.bolts.forEach((bolt) => {
        this.ctx.beginPath();
        bolt.points.forEach((p, i) => {
          if (i === 0) this.ctx.moveTo(p.x, p.y);
          else this.ctx.lineTo(p.x, p.y);
        });
        this.ctx.stroke();
      });
      this.ctx.restore();
    }

    draw(dt) {
      this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height);

      this.updateLightning(dt);

      const advance = (timer, speed, idx, seq) => {
        timer += dt;
        if (timer >= speed) {
          timer = 0;
          idx = (idx + 1) % (seq.length || 1);
        }
        return { timer, idx };
      };

      const flagAdv = advance(this.state.flagTimer, 0.24, this.state.flagIdx, this.state.flagSeq);
      this.state.flagTimer = flagAdv.timer;
      this.state.flagIdx = flagAdv.idx;

      const skelAdv = advance(this.state.skelTimer, 0.28, this.state.skelIdx, this.state.skelSeq);
      this.state.skelTimer = skelAdv.timer;
      this.state.skelIdx = skelAdv.idx;

      const boltAdv = advance(this.state.boltTimer, 0.3, this.state.boltIdx, this.state.boltSeq);
      this.state.boltTimer = boltAdv.timer;
      this.state.boltIdx = boltAdv.idx;

      const flagFrame = this.state.flagSeq[this.state.flagIdx] ?? sequences.flagCalm[0];
      const skelFrame = this.state.skelSeq[this.state.skelIdx] ?? sequences.skeletonIdle[0];
      const boltFrame = this.state.boltSeq[this.state.boltIdx];

      // Layer: flag behind, skeleton over, lightning overlay
      this.drawFrame(flagFrame, 6, 8, 0.95, 0.92);
      this.drawFrame(skelFrame, 140, 22, 0.9, 1);
      if (this.character) {
        this.character.draw(dt);
      } else {
        this.ctx.fillStyle = "#8fb7ff";
        this.ctx.font = "14px monospace";
        this.ctx.fillText("Character not loaded", 16, this.canvas.height - 16);
      }
      this.drawLightning();
      if (boltFrame !== undefined) {
        this.drawFrame(boltFrame, 250, -10, 0.9, 0.95);
      }

    }

    drawFrame(frame, dx, dy, scale = 1, alpha = 1) {
      if (!this.img) return;
      const { sx, sy } = frameToXY(frame);
      const size = FRAME_SIZE * scale;
      this.ctx.save();
      this.ctx.globalAlpha = alpha;
      this.ctx.drawImage(this.img, sx, sy, FRAME_SIZE, FRAME_SIZE, dx, dy, size, size);
      this.ctx.restore();
    }
  }

  window.SpriteSheetPlayer = {
    mount(canvas, img, payload, spearmanData) {
      const player = new SpriteSheetPlayer(canvas, img, spearmanData);
      player.updatePayload(payload);
      return player;
    },
    mountClimber(canvas, charactersData, payload) {
      const sets = loadCharacterSets(charactersData);
      const character = sets.length ? sets[0] : null;
      if (!character) return null;
      const strip = character.strips.walk || character.strips.run || character.strips.idle;
      if (!strip) return null;
      let x = -FRAME_SIZE;
      let t = 0;
      const ctx = canvas.getContext("2d");
      const maxSpeed = Math.max(10, payload.windMax || 20);
      const scale = 0.99;
      const loop = (now) => {
        const dt = Math.min(0.05, (now - t) / 1000 || 0.016);
        t = now;
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        if (!strip.ready || !strip.img.complete) {
          requestAnimationFrame(loop);
          return;
        }
        const speed = Math.max(0, payload.windMph || 0);
        const hop = Math.sin(now / 140) * 6;
        const y = canvas.height - 30 - (speed / maxSpeed) * (canvas.height - 50) + hop;
        x += dt * (40 + speed * 2.2);
        if (x > canvas.width + FRAME_SIZE) x = -FRAME_SIZE;
        const frames = strip.frames || 1;
        const idx = Math.floor((now / 140) % frames);
        const sx = idx * FRAME_SIZE;
        const dy = Math.max(10, y - FRAME_SIZE * scale);
        ctx.drawImage(strip.img, sx, 0, FRAME_SIZE, FRAME_SIZE, x, dy, FRAME_SIZE * scale, FRAME_SIZE * scale);
        requestAnimationFrame(loop);
      };
      requestAnimationFrame(loop);
      return { updatePayload(newPayload) { payload = { ...payload, ...newPayload }; } };
    },
    sequences,
    buildFrameIndex,
    resolveFrame,
    loadSpriteManifest,
    drawFrameByName,
    loadSheetImages,
    frameSize: FRAME_SIZE,
    sheetColumns: SHEET_COLS,
  };
})();
