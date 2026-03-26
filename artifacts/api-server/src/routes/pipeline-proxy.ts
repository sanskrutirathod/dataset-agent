import { Router, type IRouter, type Request, type Response, type NextFunction } from "express";
import http from "http";

const router: IRouter = Router();

const PIPELINE_HOST = "localhost";
const PIPELINE_PORT = 8000;

function streamProxyRequest(
  req: Request,
  res: Response,
  targetPath: string,
): void {
  const search = req.url.includes("?") ? req.url.substring(req.url.indexOf("?")) : "";
  const fullPath = targetPath + search;

  const outHeaders: http.OutgoingHttpHeaders = {
    ...req.headers,
    host: `${PIPELINE_HOST}:${PIPELINE_PORT}`,
  };

  const rawBody: Buffer | undefined = (req as Request & { rawBody?: Buffer }).rawBody;

  if (rawBody && rawBody.length > 0) {
    outHeaders["content-length"] = rawBody.length;
  } else {
    delete outHeaders["content-length"];
    delete outHeaders["transfer-encoding"];
  }

  const options: http.RequestOptions = {
    hostname: PIPELINE_HOST,
    port: PIPELINE_PORT,
    path: fullPath,
    method: req.method,
    headers: outHeaders,
  };

  const proxyReq = http.request(options, (proxyRes) => {
    const forwardHeaders: http.IncomingHttpHeaders = { ...proxyRes.headers };
    res.writeHead(proxyRes.statusCode ?? 200, forwardHeaders);
    proxyRes.pipe(res);
  });

  proxyReq.on("error", (err) => {
    if (!res.headersSent) {
      res.status(502).json({ error: "Pipeline API unavailable", detail: (err as Error).message });
    }
  });

  if (rawBody && rawBody.length > 0) {
    proxyReq.write(rawBody);
  }

  proxyReq.end();
}

function makePipelineHandler(pathPrefix: string) {
  return (req: Request, res: Response): void => {
    const subPath = req.path === "/" ? "" : req.path;
    streamProxyRequest(req, res, pathPrefix + subPath);
  };
}

router.use("/pipeline", makePipelineHandler("/pipeline"));
router.use("/runs", makePipelineHandler("/runs"));
router.use("/datasets", makePipelineHandler("/datasets"));
router.use("/ingest", makePipelineHandler("/ingest"));

export default router;
