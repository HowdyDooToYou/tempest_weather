(function () {
  // Compiled from static/ingest_sprite.ts (hand-transpiled for Streamlit embed)
  class FlagSprite {
    constructor(ctx, size, payload) {
      this.ctx = ctx;
      this.size = size;
      this.payload = payload;
      this.t = 0;
    }
    updatePayload(payload) {
      this.payload = payload;
    }
    draw(dt) {
      this.t += dt;
      const { windMph, lightning, ingestRate } = this.payload;
      const breeze = Math.min(1, windMph / 25);
      const stripe = (t) => 6 + Math.sin(t) * 3;
      const flagLean = breeze * 10;
      this.ctx.fillStyle = "#0b0f16";
      this.ctx.fillRect(0, 0, this.size.x, this.size.y);
      this.ctx.fillStyle = "#121824";
      this.ctx.fillRect(0, this.size.y - 30, this.size.x, 30);
      this.ctx.fillStyle = "#8a94a8";
      this.ctx.fillRect(42, 30, 6, this.size.y - 60);
      const baseY = 40;
      const flagLen = 120;
      const wiggle = Math.sin(this.t * 3 + breeze * 2) * (8 + breeze * 10);
      for (let i = 0; i < 3; i++) {
        const y = baseY + i * stripe(this.t * 0.9 + i);
        const color = i === 0 ? "#f74c7c" : i === 1 ? "#f59750" : "#ffd166";
        this.ctx.fillStyle = color;
        const sway = wiggle * (0.6 + i * 0.15);
        this.ctx.beginPath();
        this.ctx.moveTo(48, y);
        this.ctx.lineTo(48 + flagLen + sway, y + flagLean * 0.2);
        this.ctx.lineTo(48 + flagLen + sway - 10, y + stripe(this.t + i) + flagLean);
        this.ctx.lineTo(48, y + stripe(this.t + i));
        this.ctx.closePath();
        this.ctx.fill();
      }
      const ingestPct = Math.min(1, ingestRate / 50);
      this.ctx.fillStyle = "#1b2332";
      this.ctx.fillRect(this.size.x - 120, this.size.y - 26, 100, 12);
      const grad = this.ctx.createLinearGradient(this.size.x - 120, 0, this.size.x - 20, 0);
      grad.addColorStop(0, "#4bd0c2");
      grad.addColorStop(1, "#59c5ff");
      this.ctx.fillStyle = grad;
      this.ctx.fillRect(this.size.x - 120, this.size.y - 26, 100 * ingestPct, 12);
      if (lightning > 0 && Math.sin(this.t * 6) > 0.4) {
        this.ctx.fillStyle = "rgba(255,215,128,0.8)";
        this.ctx.beginPath();
        this.ctx.moveTo(this.size.x - 70, 20);
        this.ctx.lineTo(this.size.x - 50, 20);
        this.ctx.lineTo(this.size.x - 60, 70);
        this.ctx.lineTo(this.size.x - 40, 70);
        this.ctx.lineTo(this.size.x - 78, 160);
        this.ctx.lineTo(this.size.x - 62, 100);
        this.ctx.lineTo(this.size.x - 82, 100);
        this.ctx.closePath();
        this.ctx.fill();
      }
    }
  }
  class SpriteRunner {
    constructor(canvas, payload) {
      this.canvas = canvas;
      this.last = performance.now();
      this.loop = (now) => {
        const dt = Math.min(0.05, (now - this.last) / 1000);
        this.last = now;
        this.sprite.draw(dt);
        requestAnimationFrame(this.loop);
      };
      const ctx = canvas.getContext("2d");
      if (!ctx) throw new Error("Missing canvas 2d context");
      this.ctx = ctx;
      this.sprite = new FlagSprite(ctx, { x: canvas.width, y: canvas.height }, payload);
      requestAnimationFrame(this.loop);
    }
    update(payload) {
      this.sprite.updatePayload(payload);
    }
    static mount(el, payload) {
      return new SpriteRunner(el, payload);
    }
  }
  window.SpriteRunner = SpriteRunner;
})();
