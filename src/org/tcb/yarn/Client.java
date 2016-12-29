package org.tcb.yarn;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class Client {

	Configuration conf = new YarnConfiguration();

	public void run(String[] args) throws Exception {
		final int n = Integer.valueOf(args[0]);
		Path jarPath = new Path("/apps/simple/yarn-simple-test-0.1-jar-with-dependencies.jar");
		jarPath = FileSystem.get(conf).makeQualified(jarPath);

		// Create yarnClient
		YarnConfiguration conf = new YarnConfiguration();
		YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		yarnClient.start();

		// Create application via yarnClient
		YarnClientApplication app = yarnClient.createApplication();

		/**
		 * the client then set up application context, prepare the very first
		 * container of the application that contains the ApplicationMaster (AM)
		 */

		ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

		// details about the local files/jars that need to be available for your
		// application to run

		// Setup jar for ApplicationMaster
		LocalResource appMasterJar = Records.newRecord(LocalResource.class);
		setupAppMasterJar(jarPath, appMasterJar);
		amContainer.setLocalResources(Collections.singletonMap("simpleapp.jar", appMasterJar));

		// Setup CLASSPATH for ApplicationMaster / OS environment settings 
		Map<String, String> appMasterEnv = new HashMap<String, String>();
		setupAppMasterEnv(appMasterEnv);
		amContainer.setEnvironment(appMasterEnv);

		// the actual command that needs to be executed
		
//		amContainer.setCommands(
//				Collections.singletonList("$JAVA_HOME/bin/java" + " -Xmx256M" + " org.tcb.yarn.ApplicationMaster" + " "
//						+ String.valueOf(n) + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>"
//						+ ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));

		amContainer.setCommands(
				Collections.singletonList("$JAVA_HOME/bin/java" + " -Xmx256M" + " org.tcb.yarn.ApplicationMaster2" + " "
						+ String.valueOf(n) + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>"
						+ ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));
		/***********************************************************************************************/
		
		// Set up resource type requirements for ApplicationMaster
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(256);
		capability.setVirtualCores(1);

		// Finally, set-up ApplicationSubmissionContext for the application
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		appContext.setApplicationName("my-simple-yarn-app"); // application name
		appContext.setAMContainerSpec(amContainer);
		appContext.setResource(capability);
		appContext.setQueue("default"); // queue

		// Submit application
		ApplicationId appId = appContext.getApplicationId();
		System.out.println("Submitting application " + appId);
		yarnClient.submitApplication(appContext);

		ApplicationReport appReport = yarnClient.getApplicationReport(appId);
		YarnApplicationState appState = appReport.getYarnApplicationState();
		while (appState != YarnApplicationState.FINISHED && appState != YarnApplicationState.KILLED
				&& appState != YarnApplicationState.FAILED) {
			Thread.sleep(100);
			appReport = yarnClient.getApplicationReport(appId);
			appState = appReport.getYarnApplicationState();
		}

		System.out.println(
				"Application " + appId + " finished with" + " state " + appState + " at " + appReport.getFinishTime());

		yarnClient.stop();
		yarnClient.close();

	}

	private void setupAppMasterJar(Path jarPath, LocalResource appMasterJar) throws IOException {
		FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
		appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
		appMasterJar.setSize(jarStat.getLen());
		appMasterJar.setTimestamp(jarStat.getModificationTime());
		appMasterJar.setType(LocalResourceType.FILE);
		appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
	}

	private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
		// String classPathEnv = "$CLASSPATH:./*";
		String classPathEnv = "./*";
		appMasterEnv.put("CLASSPATH", classPathEnv);

		String[] defaultYarnAppClasspath = conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH);
		System.out.println("*** YARN_APPLICATION_CLASSPATH: "
				+ Arrays.asList(defaultYarnAppClasspath != null ? defaultYarnAppClasspath : new String[] {}));

		for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
			System.out.println("--> " + c);
			Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim(), File.pathSeparator);
		}

		// Apps.addToEnvironment(appMasterEnv,
		// Environment.CLASSPATH.name(),
		// Environment.PWD.$() + File.separator + "*");

		System.out.println("*** APP MASTER ENV: " + appMasterEnv);
	}

	public static void main(String[] args) throws Exception {
		Client c = new Client();
		c.run(args);
	}
}
