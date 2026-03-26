import { Router, type IRouter } from "express";
import healthRouter from "./health";
import pipelineProxyRouter from "./pipeline-proxy";

const router: IRouter = Router();

router.use(healthRouter);
router.use(pipelineProxyRouter);

export default router;
