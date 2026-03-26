import { Router, type IRouter, type Request, type Response, type NextFunction } from "express";
import http from "http";

const router: IRouter = Router();

const PIPELINE_HOST = "localhost";
const PIPELINE_PORT = 8000;

function proxyToPipeline(req: Request, res: Response): void {
  const search = req.url.includes("?") ? req.url.substring(req.url.indexOf("?")) : "";
  const subPath = req.path === "/" ? "" : req.path;
  const fullPath = "/pipeline" + subPath + search;

  const options: http.RequestOptions = {
    hostname: PIPELINE_HOST,
    port: PIPELINE_PORT,
    path: fullPath,
    method: req.method,
    headers: {
      ...req.headers,
      host: `${PIPELINE_HOST}:${PIPELINE_PORT}`,
    },
  };

  const proxyReq = http.request(options, (proxyRes) => {
    const contentDisposition = proxyRes.headers["content-disposition"];
    if (contentDisposition) {
      res.setHeader("content-disposition", contentDisposition);
    }
    res.writeHead(proxyRes.statusCode ?? 200, proxyRes.headers);
    proxyRes.pipe(res);
  });

  proxyReq.on("error", (err) => {
    res.status(502).json({ error: "Pipeline API unavailable", detail: (err as Error).message });
  });

  if (req.body && Object.keys(req.body).length > 0) {
    const body = JSON.stringify(req.body);
    proxyReq.setHeader("content-type", "application/json");
    proxyReq.setHeader("content-length", Buffer.byteLength(body));
    proxyReq.write(body);
  }

  proxyReq.end();
}

router.use("/pipeline", (req: Request, res: Response, next: NextFunction) => {
  proxyToPipeline(req, res);
});

export default router;
