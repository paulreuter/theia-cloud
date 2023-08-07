package org.eclipse.theia.cloud.operator.handler.util;

import static org.eclipse.theia.cloud.common.util.LogMessageUtil.formatLogMessage;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.theia.cloud.common.k8s.client.TheiaCloudClient;
import org.eclipse.theia.cloud.common.k8s.resource.AppDefinition;
import org.eclipse.theia.cloud.common.k8s.resource.AppDefinitionSpec;
import org.eclipse.theia.cloud.operator.handler.BandwidthLimiter;
import org.eclipse.theia.cloud.operator.handler.DeploymentTemplateReplacements;
import org.eclipse.theia.cloud.operator.handler.IngressPathProvider;
import org.eclipse.theia.cloud.operator.handler.impl.AddedHandlerUtil;
import org.eclipse.theia.cloud.operator.handler.impl.EagerStartAppDefinitionAddedHandler;
import org.eclipse.theia.cloud.operator.util.JavaResourceUtil;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;

public final class TheiaCloudInstanceUtil {

    public static final Logger LOGGER = LogManager.getLogger(TheiaCloudInstanceUtil.class);

    public static void createAndApplyEmailConfigMap(NamespacedKubernetesClient client, String namespace,
	    String correlationId, String appDefinitionResourceName, String appDefinitionResourceUID, int instance,
	    AppDefinition appDefinition) {
	Map<String, String> replacements = TheiaCloudConfigMapUtil.getEmailConfigMapReplacements(namespace,
		appDefinition, instance);
	String configMapYaml;
	try {
	    configMapYaml = JavaResourceUtil.readResourceAndReplacePlaceholders(
		    AddedHandlerUtil.TEMPLATE_CONFIGMAP_EMAILS_YAML, replacements, correlationId);
	} catch (IOException | URISyntaxException e) {
	    LOGGER.error(
		    formatLogMessage(correlationId, "Error while adjusting template for instance number " + instance),
		    e);
	    return;
	}
	K8sUtil.loadAndCreateConfigMapWithOwnerReference(client, namespace, correlationId, configMapYaml,
		AppDefinitionSpec.API, AppDefinitionSpec.KIND, appDefinitionResourceName, appDefinitionResourceUID, 0);
    }

    public static void createAndApplyProxyConfigMap(NamespacedKubernetesClient client, String namespace,
	    String correlationId, String appDefinitionResourceName, String appDefinitionResourceUID, int instance,
	    AppDefinition appDefinition, IngressPathProvider ingressPathProvider) {
	Map<String, String> replacements = TheiaCloudConfigMapUtil.getProxyConfigMapReplacements(namespace,
		appDefinition, instance);
	String configMapYaml;
	try {
	    configMapYaml = JavaResourceUtil.readResourceAndReplacePlaceholders(
		    AddedHandlerUtil.TEMPLATE_CONFIGMAP_YAML, replacements, correlationId);
	} catch (IOException | URISyntaxException e) {
	    LOGGER.error(
		    formatLogMessage(correlationId, "Error while adjusting template for instance number " + instance),
		    e);
	    return;
	}
	K8sUtil.loadAndCreateConfigMapWithOwnerReference(client, namespace, correlationId, configMapYaml,
		AppDefinitionSpec.API, AppDefinitionSpec.KIND, appDefinitionResourceName, appDefinitionResourceUID, 0,
		configMap -> {
		    String host = appDefinition.getSpec().getHost()
			    + ingressPathProvider.getPath(appDefinition, instance);
		    int port = appDefinition.getSpec().getPort();
		    AddedHandlerUtil.updateProxyConfigMap(client, namespace, configMap, host, port);
		});
    }

    public static void createAndApplyDeployment(NamespacedKubernetesClient client, String namespace,
	    String correlationId, String appDefinitionResourceName, String appDefinitionResourceUID, int instance,
	    AppDefinition appDefinition, DeploymentTemplateReplacements deploymentReplacements,
	    BandwidthLimiter bandwidthLimiter, boolean useOAuth2Proxy) {
	Map<String, String> replacements = deploymentReplacements.getReplacements(namespace, appDefinition, instance);
	LOGGER.trace(formatLogMessage(correlationId, "Replacements for deployment: " + replacements.toString()));

	String templateYaml = useOAuth2Proxy ? AddedHandlerUtil.TEMPLATE_DEPLOYMENT_YAML
		: AddedHandlerUtil.TEMPLATE_DEPLOYMENT_WITHOUT_AOUTH2_PROXY_YAML;
	String deploymentYaml;
	try {
	    deploymentYaml = JavaResourceUtil.readResourceAndReplacePlaceholders(templateYaml, replacements,
		    correlationId);
	} catch (IOException | URISyntaxException e) {
	    LOGGER.error(
		    formatLogMessage(correlationId, "Error while adjusting template for instance number " + instance),
		    e);
	    return;
	}
	K8sUtil.loadAndCreateDeploymentWithOwnerReference(client, namespace, correlationId, deploymentYaml,
		AppDefinitionSpec.API, AppDefinitionSpec.KIND, appDefinitionResourceName, appDefinitionResourceUID, 0,
		deployment -> {
		    bandwidthLimiter.limit(deployment, appDefinition.getSpec().getDownlinkLimit(),
			    appDefinition.getSpec().getUplinkLimit(), correlationId);
		    AddedHandlerUtil.removeEmptyResources(deployment);
		    if (appDefinition.getSpec().getPullSecret() != null
			    && !appDefinition.getSpec().getPullSecret().isEmpty()) {
			AddedHandlerUtil.addImagePullSecret(deployment, appDefinition.getSpec().getPullSecret());
		    }
		});
    }

    public static void createAndApplyService(NamespacedKubernetesClient client, String namespace, String correlationId,
	    String appDefinitionResourceName, String appDefinitionResourceUID, int instance,
	    AppDefinition appDefinition, boolean useOAuth2Proxy) {
	Map<String, String> replacements = TheiaCloudServiceUtil.getServiceReplacements(namespace, appDefinition,
		instance);
	String templateYaml = useOAuth2Proxy ? AddedHandlerUtil.TEMPLATE_SERVICE_YAML
		: AddedHandlerUtil.TEMPLATE_SERVICE_WITHOUT_AOUTH2_PROXY_YAML;
	String serviceYaml;
	try {
	    serviceYaml = JavaResourceUtil.readResourceAndReplacePlaceholders(templateYaml, replacements,
		    correlationId);
	} catch (IOException | URISyntaxException e) {
	    LOGGER.error(
		    formatLogMessage(correlationId, "Error while adjusting template for instance number " + instance),
		    e);
	    return;
	}
	K8sUtil.loadAndCreateServiceWithOwnerReference(client, namespace, correlationId, serviceYaml,
		AppDefinitionSpec.API, AppDefinitionSpec.KIND, appDefinitionResourceName, appDefinitionResourceUID, 0);
    }

    public static boolean ensureInstances(AppDefinition appDefinition, TheiaCloudClient client,
	    DeploymentTemplateReplacements deploymentReplacements, BandwidthLimiter bandwidthLimiter,
	    IngressPathProvider ingressPathProvider, String correlationId, int instances, boolean useKeycloak) {
	String appDefinitionResourceName = appDefinition.getMetadata().getName();
	String appDefinitionResourceUID = appDefinition.getMetadata().getUid();

	/* Get existing services for this app definition */
	List<Service> existingServices = K8sUtil.getExistingServices(client.kubernetes(), client.namespace(),
		appDefinitionResourceName, appDefinitionResourceUID);
	LOGGER.trace(formatLogMessage(correlationId, "Existing services: " + existingServices.toString()));

	/* Compute missing services */
	Set<Integer> missingServiceIds = TheiaCloudServiceUtil.computeIdsOfMissingServices(appDefinition, correlationId,
		instances, existingServices);

	LOGGER.trace(formatLogMessage(correlationId, "Missing serviceIds: " + missingServiceIds.toString()));
	Set<Integer> expectedIds = TheiaCloudServiceUtil.computeIdsOfExistingServices(appDefinition, correlationId,
		existingServices);
	expectedIds.addAll(missingServiceIds);
	LOGGER.trace(formatLogMessage(correlationId, "All expected serviceIds: " + expectedIds.toString()));

	/* Create missing services for this app definition */
	for (int instance : missingServiceIds) {
	    createAndApplyService(client.kubernetes(), client.namespace(), correlationId, appDefinitionResourceName,
		    appDefinitionResourceUID, instance, appDefinition, useKeycloak);
	}

	if (useKeycloak) {
	    /* Get existing configmaps for this app definition */
	    List<ConfigMap> existingConfigMaps = K8sUtil.getExistingConfigMaps(client.kubernetes(), client.namespace(),
		    appDefinitionResourceName, appDefinitionResourceUID);
	    List<ConfigMap> existingProxyConfigMaps = existingConfigMaps.stream()//
		    .filter(configmap -> EagerStartAppDefinitionAddedHandler.LABEL_VALUE_PROXY.equals(
			    configmap.getMetadata().getLabels().get(EagerStartAppDefinitionAddedHandler.LABEL_KEY)))//
		    .collect(Collectors.toList());
	    List<ConfigMap> existingEmailsConfigMaps = existingConfigMaps.stream()//
		    .filter(configmap -> EagerStartAppDefinitionAddedHandler.LABEL_VALUE_EMAILS.equals(
			    configmap.getMetadata().getLabels().get(EagerStartAppDefinitionAddedHandler.LABEL_KEY)))//
		    .collect(Collectors.toList());

	    /* Compute missing configmaps */
	    Set<Integer> missingProxyIds = TheiaCloudConfigMapUtil.computeIdsOfMissingProxyConfigMaps(appDefinition,
		    correlationId, expectedIds, existingProxyConfigMaps);
	    Set<Integer> missingEmailIds = TheiaCloudConfigMapUtil.computeIdsOfMissingEmailConfigMaps(appDefinition,
		    correlationId, expectedIds, existingEmailsConfigMaps);

	    /* Create missing configmaps for this app definition */
	    for (int instance : missingProxyIds) {
		createAndApplyProxyConfigMap(client.kubernetes(), client.namespace(), correlationId,
			appDefinitionResourceName, appDefinitionResourceUID, instance, appDefinition,
			ingressPathProvider);
	    }
	    for (int instance : missingEmailIds) {
		createAndApplyEmailConfigMap(client.kubernetes(), client.namespace(), correlationId,
			appDefinitionResourceName, appDefinitionResourceUID, instance, appDefinition);
	    }
	}

	/* Get existing deployments for this app definition */
	List<Deployment> existingDeployments = K8sUtil.getExistingDeployments(client.kubernetes(), client.namespace(),
		appDefinitionResourceName, appDefinitionResourceUID);

	LOGGER.trace(formatLogMessage(correlationId, "Existing deployments: " + existingDeployments.toString()));

	/* Compute missing deployments */
	Set<Integer> missingDeploymentIds = TheiaCloudDeploymentUtil.computeIdsOfMissingDeployments(appDefinition,
		correlationId, expectedIds, existingDeployments);

	LOGGER.trace(formatLogMessage(correlationId, "Missing deployment ids: " + missingDeploymentIds.toString()));

	/* Create missing deployments for this app definition */
	for (int instance : missingDeploymentIds) {
	    createAndApplyDeployment(client.kubernetes(), client.namespace(), correlationId, appDefinitionResourceName,
		    appDefinitionResourceUID, instance, appDefinition, deploymentReplacements, bandwidthLimiter,
		    useKeycloak);
	}
	return true;
    }
}