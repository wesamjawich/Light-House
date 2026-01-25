from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class EmbeddingConfig:
    model_name: str = "ViT-B-32"
    pretrained: str = "openai"
    dim: int = 512
    device: str = "auto"  # "auto" | "cpu" | "cuda" | "mps"


class EmbedderUnavailable(RuntimeError):
    pass


class ClipEmbedder:
    def __init__(self, cfg: EmbeddingConfig) -> None:
        self.cfg = cfg
        self._loaded = False
        self._model = None
        self._preprocess = None
        self._tokenizer = None
        self._device = None
        import threading

        self._lock = threading.Lock()

    @property
    def model_id(self) -> str:
        return f"open_clip:{self.cfg.model_name}:{self.cfg.pretrained}"

    @property
    def device(self) -> str:
        if self._device:
            return str(self._device)
        return self._select_device()

    def _select_device(self) -> str:
        if self.cfg.device != "auto":
            return self.cfg.device
        try:
            import torch

            if torch.cuda.is_available():
                return "cuda"
            if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
                return "mps"
        except Exception:
            pass
        return "cpu"

    def _ensure_loaded(self) -> None:
        with self._lock:
            if self._loaded:
                return
            try:
                import open_clip
            except Exception as e:
                raise EmbedderUnavailable(
                    "CLIP dependencies not installed. Install with: pip install -e \".[clip]\""
                ) from e

            device = self._select_device()
            model, _, preprocess = open_clip.create_model_and_transforms(
                self.cfg.model_name, pretrained=self.cfg.pretrained, device=device
            )
            tokenizer = open_clip.get_tokenizer(self.cfg.model_name)

            self._model = model
            self._preprocess = preprocess
            self._tokenizer = tokenizer
            self._device = device
            self._loaded = True

    def image_dim(self) -> int:
        return int(self.cfg.dim)

    def embed_text(self, text: str):
        self._ensure_loaded()
        import numpy as np
        import torch

        assert self._model is not None and self._tokenizer is not None
        with self._lock:
            tokens = self._tokenizer([text]).to(self._device)
            with torch.no_grad():
                feats = self._model.encode_text(tokens)
                feats = feats / feats.norm(dim=-1, keepdim=True)
        return np.asarray(feats[0].cpu(), dtype="float32")

    def embed_image(self, path: Path):
        self._ensure_loaded()
        import numpy as np
        import torch
        from PIL import Image

        assert self._model is not None and self._preprocess is not None
        with self._lock:
            with Image.open(path) as img:
                img = img.convert("RGB")
                image_tensor = self._preprocess(img).unsqueeze(0).to(self._device)
            with torch.no_grad():
                feats = self._model.encode_image(image_tensor)
                feats = feats / feats.norm(dim=-1, keepdim=True)
        return np.asarray(feats[0].cpu(), dtype="float32")


def build_embedder(
    enabled: bool,
    *,
    model_name: Optional[str] = None,
    pretrained: Optional[str] = None,
    device: Optional[str] = None,
) -> Optional[ClipEmbedder]:
    if not enabled:
        return None
    cfg = EmbeddingConfig(
        model_name=model_name or EmbeddingConfig.model_name,
        pretrained=pretrained or EmbeddingConfig.pretrained,
        device=device or EmbeddingConfig.device,
    )
    return ClipEmbedder(cfg)
