/********************************************************************************
 * Copyright (C) 2022 EclipseSource, Lockular, Ericsson, STMicroelectronics and 
 * others.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v. 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is available at
 * https://www.gnu.org/software/classpath/license.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 ********************************************************************************/
package org.eclipse.theia.cloud.operator.handler.impl;

import static org.eclipse.theia.cloud.common.util.LogMessageUtil.formatLogMessage;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.theia.cloud.common.k8s.client.TheiaCloudClient;
import org.eclipse.theia.cloud.common.k8s.resource.AppDefinition;
import org.eclipse.theia.cloud.common.k8s.resource.Session;
import org.eclipse.theia.cloud.common.k8s.resource.SessionSpec;
import org.eclipse.theia.cloud.common.util.JavaUtil;
import org.eclipse.theia.cloud.operator.TheiaCloudArguments;
import org.eclipse.theia.cloud.operator.handler.BandwidthLimiter;
import org.eclipse.theia.cloud.operator.handler.DeploymentTemplateReplacements;
import org.eclipse.theia.cloud.operator.handler.IngressPathProvider;
import org.eclipse.theia.cloud.operator.handler.SessionHandler;
import org.eclipse.theia.cloud.operator.handler.util.K8sUtil;
import org.eclipse.theia.cloud.operator.handler.util.TheiaCloudConfigMapUtil;
import org.eclipse.theia.cloud.operator.handler.util.TheiaCloudDeploymentUtil;
import org.eclipse.theia.cloud.operator.handler.util.TheiaCloudHandlerUtil;
import org.eclipse.theia.cloud.operator.handler.util.TheiaCloudIngressUtil;
import org.eclipse.theia.cloud.operator.handler.util.TheiaCloudInstanceUtil;
import org.eclipse.theia.cloud.operator.handler.util.TheiaCloudK8sUtil;
import org.eclipse.theia.cloud.operator.handler.util.TheiaCloudServiceUtil;

import com.google.inject.Inject;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.HTTPIngressPath;
import io.fabric8.kubernetes.api.model.networking.v1.HTTPIngressRuleValue;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressBackend;
import io.fabric8.kubernetes.api.model.networking.v1.IngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.IngressServiceBackend;
import io.fabric8.kubernetes.api.model.networking.v1.ServiceBackendPort;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;

/**
 * A {@link SessionAddedHandler} that relies on the fact that the app definition
 * handler created spare deployments to use.
 */
public class EagerStartSessionHandler implements SessionHandler {

    private static final Logger LOGGER = LogManager.getLogger(EagerStartSessionHandler.class);

    public static final String LABEL_KEY = "session";

    @Inject
    private TheiaCloudClient client;

    @Inject
    protected IngressPathProvider ingressPathProvider;

    @Inject
    protected TheiaCloudArguments arguments;

    @Inject
    protected BandwidthLimiter bandwidthLimiter;

    @Inject
    protected DeploymentTemplateReplacements deploymentReplacements;

    @Override
    public boolean sessionAdded(Session session, String correlationId) {
	SessionSpec spec = session.getSpec();
	LOGGER.info(formatLogMessage(correlationId, "Handling " + spec));

	String sessionResourceName = session.getMetadata().getName();
	String sessionResourceUID = session.getMetadata().getUid();

	String appDefinitionID = spec.getAppDefinition();
	String userEmail = spec.getUser();

	/* find app definition for session */
	Optional<AppDefinition> appDefinition = client.appDefinitions().get(appDefinitionID);
	if (appDefinition.isEmpty()) {
	    LOGGER.error(formatLogMessage(correlationId, "No App Definition with name " + appDefinitionID + " found."));
	    return false;
	}

	String appDefinitionResourceName = appDefinition.get().getMetadata().getName();
	String appDefinitionResourceUID = appDefinition.get().getMetadata().getUid();
	int port = appDefinition.get().getSpec().getPort();

	/* find ingress */
	Optional<Ingress> ingress = K8sUtil.getExistingIngress(client.kubernetes(), client.namespace(),
		appDefinitionResourceName, appDefinitionResourceUID);
	if (ingress.isEmpty()) {
	    LOGGER.error(
		    formatLogMessage(correlationId, "No Ingress for app definition " + appDefinitionID + " found."));
	    return false;
	}

	if (!ensureInstances(appDefinition.get(), correlationId))
	    return false;

	/* get a service to use */
	Entry<Optional<Service>, Boolean> reserveServiceResult = reserveService(client.kubernetes(), client.namespace(),
		appDefinitionResourceName, appDefinitionResourceUID, appDefinitionID, sessionResourceName,
		sessionResourceUID, correlationId);
	if (reserveServiceResult.getValue()) {
	    LOGGER.info(formatLogMessage(correlationId, "Found an already reserved service"));
	    return true;
	}
	Optional<Service> serviceToUse = reserveServiceResult.getKey();
	if (serviceToUse.isEmpty()) {
	    LOGGER.error(
		    formatLogMessage(correlationId, "No Service for app definition " + appDefinitionID + " found."));
	    return false;
	}

	/* get the deployment for the service and add as owner */
	Integer instance = TheiaCloudServiceUtil.getId(correlationId, appDefinition.get(), serviceToUse.get());
	if (instance == null) {
	    LOGGER.error(formatLogMessage(correlationId, "Error while getting instance from Service"));
	    return false;
	}

	try {
	    client.kubernetes().apps().deployments()
		    .withName(TheiaCloudDeploymentUtil.getDeploymentName(appDefinition.get(), instance))
		    .edit(deployment -> {
			TheiaCloudHandlerUtil.addLabelToItem(correlationId, LABEL_KEY, sessionResourceUID, deployment);
			return TheiaCloudHandlerUtil.addOwnerReferenceToItem(correlationId, sessionResourceName,
				sessionResourceUID, deployment);
		    });
	} catch (KubernetesClientException e) {
	    LOGGER.error(formatLogMessage(correlationId, "Error while editing deployment "
		    + (appDefinitionID + TheiaCloudDeploymentUtil.DEPLOYMENT_NAME + instance)), e);
	    return false;
	}

	if (arguments.isUseKeycloak()) {
	    /* add user to allowed emails */
	    try {
		client.kubernetes().configMaps()
			.withName(TheiaCloudConfigMapUtil.getEmailConfigName(appDefinition.get(), instance))
			.edit(configmap -> {
			    configmap.setData(Collections
				    .singletonMap(AddedHandlerUtil.FILENAME_AUTHENTICATED_EMAILS_LIST, userEmail));
			    TheiaCloudHandlerUtil.addLabelToItem(correlationId, LABEL_KEY, sessionResourceUID,
				    configmap);
			    return configmap;
			});
		client.kubernetes().configMaps()
			.withName(TheiaCloudConfigMapUtil.getProxyConfigName(appDefinition.get(), instance))
			.edit(configmap -> {
			    TheiaCloudHandlerUtil.addLabelToItem(correlationId, LABEL_KEY, sessionResourceUID,
				    configmap);
			    return configmap;
			});
	    } catch (KubernetesClientException e) {
		LOGGER.error(
			formatLogMessage(correlationId,
				"Error while editing email configmap "
					+ (appDefinitionID + TheiaCloudConfigMapUtil.CONFIGMAP_EMAIL_NAME + instance)),
			e);
		return false;
	    }
	}

	/* adjust the ingress */
	String host;
	try {
	    host = updateIngress(ingress, serviceToUse, appDefinitionID, instance, port, appDefinition.get(),
		    correlationId);
	} catch (KubernetesClientException e) {
	    LOGGER.error(formatLogMessage(correlationId,
		    "Error while editing ingress " + ingress.get().getMetadata().getName()), e);
	    return false;
	}

	/* Update session resource */
	try {
	    AddedHandlerUtil.updateSessionURLAsync(client.kubernetes(), session, client.namespace(), host,
		    correlationId);
	} catch (KubernetesClientException e) {
	    LOGGER.error(
		    formatLogMessage(correlationId, "Error while editing session " + session.getMetadata().getName()),
		    e);
	    return false;
	}

	return true;
    }

    private boolean ensureInstances(AppDefinition appDefinition, String correlationId) {
	long currentInstances = TheiaCloudK8sUtil.getCurrentInstancesNumber(client.kubernetes(), client.namespace(),
		appDefinition.getSpec(), correlationId);
	int requestedInstances = (int) Math.max(currentInstances, appDefinition.getSpec().getMaxInstances());
	return TheiaCloudInstanceUtil.ensureInstances(appDefinition, client, deploymentReplacements, bandwidthLimiter,
		ingressPathProvider, correlationId, requestedInstances, false);
    }

    protected synchronized Entry<Optional<Service>, Boolean> reserveService(NamespacedKubernetesClient client,
	    String namespace, String appDefinitionResourceName, String appDefinitionResourceUID, String appDefinitionID,
	    String sessionResourceName, String sessionResourceUID, String correlationId) {
	List<Service> existingServices = K8sUtil.getExistingServices(client, namespace, appDefinitionResourceName,
		appDefinitionResourceUID);

	Optional<Service> alreadyReservedService = TheiaCloudServiceUtil.getServiceOwnedBySession(sessionResourceName,
		sessionResourceUID, existingServices);
	if (alreadyReservedService.isPresent()) {
	    return JavaUtil.tuple(alreadyReservedService, true);
	}

	Optional<Service> serviceToUse = TheiaCloudServiceUtil.getUnusedService(existingServices);
	if (serviceToUse.isEmpty()) {
	    return JavaUtil.tuple(serviceToUse, false);
	}

	/* add our session as owner to the service */
	try {
	    client.services().inNamespace(namespace).withName(serviceToUse.get().getMetadata().getName())
		    .edit(service -> {
			TheiaCloudHandlerUtil.addLabelToItem(correlationId, LABEL_KEY, sessionResourceUID, service);
			return TheiaCloudHandlerUtil.addOwnerReferenceToItem(correlationId, sessionResourceName,
				sessionResourceUID, service);
		    });

	} catch (KubernetesClientException e) {
	    LOGGER.error(formatLogMessage(correlationId,
		    "Error while editing service " + (serviceToUse.get().getMetadata().getName())), e);
	    return JavaUtil.tuple(Optional.empty(), false);
	}
	return JavaUtil.tuple(serviceToUse, false);
    }

    protected synchronized String updateIngress(Optional<Ingress> ingress, Optional<Service> serviceToUse,
	    String appDefinitionID, int instance, int port, AppDefinition appDefinition, String correlationId) {
	String host = appDefinition.getSpec().getHost();
	String path = ingressPathProvider.getPath(appDefinition, instance);
	client.ingresses().edit(correlationId, ingress.get().getMetadata().getName(),
		ingressToUpdate -> addIngressRule(ingressToUpdate, serviceToUse.get(), host, port, path));
	return host + path + "/";
    }

    protected Ingress addIngressRule(Ingress ingress, Service serviceToUse, String host, int port, String path) {
	IngressRule ingressRule = new IngressRule();
	ingress.getSpec().getRules().add(ingressRule);

	ingressRule.setHost(host);

	HTTPIngressRuleValue http = new HTTPIngressRuleValue();
	ingressRule.setHttp(http);

	HTTPIngressPath httpIngressPath = new HTTPIngressPath();
	http.getPaths().add(httpIngressPath);
	httpIngressPath.setPath(path + AddedHandlerUtil.INGRESS_REWRITE_PATH);
	httpIngressPath.setPathType("Prefix");

	IngressBackend ingressBackend = new IngressBackend();
	httpIngressPath.setBackend(ingressBackend);

	IngressServiceBackend ingressServiceBackend = new IngressServiceBackend();
	ingressBackend.setService(ingressServiceBackend);
	ingressServiceBackend.setName(serviceToUse.getMetadata().getName());

	ServiceBackendPort serviceBackendPort = new ServiceBackendPort();
	ingressServiceBackend.setPort(serviceBackendPort);
	serviceBackendPort.setNumber(port);

	return ingress;
    }

    @Override
    public boolean sessionDeleted(Session session, String correlationId) {
	/* session information */
	SessionSpec sessionSpec = session.getSpec();

	/* find appDefinition for session */
	String appDefinitionID = sessionSpec.getAppDefinition();

	Optional<AppDefinition> appDefinition = client.appDefinitions().get(appDefinitionID);
	if (appDefinition.isEmpty()) {
	    LOGGER.error(formatLogMessage(correlationId, "No App Definition with name " + appDefinitionID + " found."));
	    return false;
	}
	String appDefinitionResourceName = appDefinition.get().getMetadata().getName();
	String appDefinitionResourceUID = appDefinition.get().getMetadata().getUid();

	/* find ingress */
	Optional<Ingress> ingress = K8sUtil.getExistingIngress(client.kubernetes(), client.namespace(),
		appDefinitionResourceName, appDefinitionResourceUID);
	if (ingress.isEmpty()) {
	    LOGGER.error(
		    formatLogMessage(correlationId, "No Ingress for app definition " + appDefinitionID + " found."));
	    return false;
	}

	String sessionResourceName = session.getMetadata().getName();
	String sessionResourceUID = session.getMetadata().getUid();

	List<Service> existingServices = K8sUtil.getExistingServices(client.kubernetes(), client.namespace(),
		appDefinitionResourceName, appDefinitionResourceUID, LABEL_KEY, sessionResourceUID);
	Integer instanceNumber = null;
	if (existingServices.isEmpty())
	    LOGGER.trace(formatLogMessage(correlationId, "Session has no service."));
	else
	    instanceNumber = TheiaCloudServiceUtil.getId(correlationId, appDefinition.get(), existingServices.get(0));
	List<ConfigMap> existingConfigMaps = K8sUtil.getExistingConfigMaps(client.kubernetes(), client.namespace(),
		appDefinitionResourceName, appDefinitionResourceUID, LABEL_KEY, sessionResourceUID);
	if (existingConfigMaps.isEmpty())
	    LOGGER.trace(formatLogMessage(correlationId, "Session has no config maps."));
	else if (instanceNumber == null)
	    instanceNumber = TheiaCloudConfigMapUtil.getProxyId(correlationId, appDefinition.get(),
		    existingConfigMaps.get(0));
	List<Deployment> existingDeployments = K8sUtil.getExistingDeployments(client.kubernetes(), client.namespace(),
		appDefinitionResourceName, appDefinitionResourceUID, LABEL_KEY, sessionResourceUID);
	if (existingDeployments.isEmpty())
	    LOGGER.trace(formatLogMessage(correlationId, "Session has no deployments."));
	else if (instanceNumber == null)
	    instanceNumber = TheiaCloudDeploymentUtil.getId(correlationId, appDefinition.get(),
		    existingDeployments.get(0));

	if (instanceNumber != null) {
	    String path = ingressPathProvider.getPath(appDefinition.get(), instanceNumber);
	    LOGGER.trace(formatLogMessage(correlationId, "Removing ingress rule for path: " + path));
	    TheiaCloudIngressUtil.removeIngressRule(client.kubernetes(), client.namespace(), ingress.get(), path,
		    correlationId);
	} else
	    LOGGER.error(formatLogMessage(correlationId, "Could not determine instance number for cleanup of session"));

	long currentInstances = TheiaCloudK8sUtil.getCurrentInstancesNumber(client.kubernetes(), client.namespace(),
		appDefinition.get().getSpec(), correlationId);

	LOGGER.trace(formatLogMessage(correlationId, "Counting existing sessions: " + currentInstances + " found."));

	if (currentInstances > appDefinition.get().getSpec().getMinInstances()) {
	    // fully delete deployment
	    for (Service service : existingServices) {
		LOGGER.trace(formatLogMessage(correlationId, "Deleting service of session: " + service.toString()));
		client.kubernetes().services().inNamespace(client.namespace()).delete(service);
	    }
	    // delete config maps
	    for (ConfigMap configMap : existingConfigMaps) {
		LOGGER.trace(
			formatLogMessage(correlationId, "Deleting config maps of session: " + configMap.toString()));
		client.kubernetes().configMaps().inNamespace(client.namespace()).delete(configMap);
	    }
	    // delete deployment
	    for (Deployment deployment : existingDeployments) {
		LOGGER.trace(
			formatLogMessage(correlationId, "Deleting deployments of session: " + deployment.toString()));
		client.kubernetes().apps().deployments().inNamespace(client.namespace()).delete(deployment);
	    }
	} else {
	    for (Service service : existingServices) {
		LOGGER.trace(formatLogMessage(correlationId,
			"Removing ownership from service of session: " + service.toString()));
		client.kubernetes().services().inNamespace(client.namespace()).withName(service.getMetadata().getName())
			.edit(editedService -> {
			    editedService.getMetadata().getLabels().remove(LABEL_KEY);
			    return TheiaCloudHandlerUtil.removeOwnerReferenceFromItem(correlationId,
				    sessionResourceName, sessionResourceUID, service);
			});

	    }
	    for (ConfigMap configMap : existingConfigMaps) {
		LOGGER.trace(formatLogMessage(correlationId,
			"Removing ownership of config maps of session: " + configMap.toString()));
		client.kubernetes().configMaps().withName(configMap.getMetadata().getName()).edit(configmap -> {
		    if (configmap.getData().get(AddedHandlerUtil.FILENAME_AUTHENTICATED_EMAILS_LIST) != null)
			configmap.getData().put(AddedHandlerUtil.FILENAME_AUTHENTICATED_EMAILS_LIST, "");
		    configmap.getMetadata().getLabels().remove(LABEL_KEY);
		    return configmap;
		});
	    }
	    // restart container
	    for (Deployment deployment : existingDeployments) {
		LOGGER.trace(
			formatLogMessage(correlationId, "Restarting deployments of session: " + deployment.toString()));
		deployment.getMetadata().getLabels().remove(LABEL_KEY);
		client.kubernetes().apps().deployments().inNamespace(client.namespace())
			.withName(deployment.getMetadata().getName()).rolling().restart();
	    }

	}
	return true;
    }
}
