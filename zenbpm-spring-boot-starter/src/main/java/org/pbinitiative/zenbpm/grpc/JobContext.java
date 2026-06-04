package org.pbinitiative.zenbpm.grpc;

import org.pbinitiative.zenbpm.proto.Zenbpm;

import java.util.Map;

public class JobContext {
    private Zenbpm.WaitingJob waitingJob;
    private Map<String, Object> variables;

    public JobContext(Zenbpm.WaitingJob waitingJob, Map<String, Object> variables) {
        this.waitingJob = waitingJob;
        this.variables = variables;
    }

    public Zenbpm.WaitingJob getWaitingJob() {
        return waitingJob;
    }

    public void setWaitingJob(Zenbpm.WaitingJob waitingJob) {
        this.waitingJob = waitingJob;
    }

    public Map<String, Object> getVariables() {
        return variables;
    }

    public void setVariables(Map<String, Object> variables) {
        this.variables = variables;
    }
}
