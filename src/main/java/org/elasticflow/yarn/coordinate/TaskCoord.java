package org.elasticflow.yarn.coordinate;

import java.io.IOException;

import org.elasticflow.task.TaskStateControl;

public class TaskCoord {
	
	public void start() {
		new Thread(new Runnable() {
            public void run() {
                try {
                    TasksProvider provider = new TasksProvider(8088);
                    provider.register(Coordination.class, TaskStateControl.class);
                    provider.start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();    
    }
}
