/*
 * The MIT License
 * Copyright (c) 2012 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package microsoft.exchange.webservices.data;

import javax.xml.stream.XMLStreamException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

/**
 * Represents a binding to the Exchange Autodiscover Service.
 */
public final class AutodiscoverService extends ExchangeServiceBase {

  /**
   * The domain.
   */
  private String domain;

  /**
   * The url.
   */
  private URI url;

  /**
   * The redirection url validation callback.
   */
  private IAutodiscoverRedirectionUrl
      redirectionUrlValidationCallback;

  /**
   * The dns client.
   */
  private AutodiscoverDnsClient dnsClient;

  /**
   * The dns server address.
   */
  private String dnsServerAddress;

  /**
   * Autodiscover legacy path
   */
  private static final String AutodiscoverLegacyPath =
      "/autodiscover/autodiscover.xml";

  /**
   * Autodiscover legacy HTTPS Url
   */
  private static final String AutodiscoverLegacyHttpsUrl = "https://%s" +
      AutodiscoverLegacyPath;

  /**
   * Autodiscover legacy HTTP Url
   */
  private static final String AutodiscoverLegacyHttpUrl = "http://%s" +
      AutodiscoverLegacyPath;

  /**
   * Autodiscover SOAP HTTPS Url
   */
  private static final String AutodiscoverSoapHttpsUrl =
      "https://%s/autodiscover/autodiscover.svc";

  /**
   * Autodiscover request namespace
   */
  private static final String AutodiscoverRequestNamespace =
      "http://schemas.microsoft.com/exchange/autodiscover/" +
          "outlook/requestschema/2006";

  /**
   * Maximum number of Url (or address) redirections that will be followed by an Autodiscover call
   */
  private static final int AutodiscoverMaxRedirections = 10;

  /**
   * HTTP header indicating that SOAP Autodiscover service is enabled.
   */
  private static final String AutodiscoverSoapEnabledHeaderName =
      "X-SOAP-Enabled";

  /**
   * HTTP header indicating that WS-Security Autodiscover service is enabled.
   */
  private static final String AutodiscoverWsSecurityEnabledHeaderName =
      "X-WSSecurity-Enabled";

  /**
   * Minimum request version for Autodiscover SOAP service.
   */
  private static final ExchangeVersion
      MinimumRequestVersionForAutoDiscoverSoapService =
      ExchangeVersion.Exchange2010;

  /**
   * Hardcoded O365 vars, as O365 discovery is borked
   */
  private static final String o365AutodiscoverHost = "autodiscover-s.outlook.com";

  private static final String o365EwsUrl = "https://outlook.office365.com/EWS/Exchange.asmx";

  // Legacy Autodiscover

  /**
   * Calls the Autodiscover service to get configuration settings at the
   * specified URL.
   *
   * @param <TSettings>  the generic type
   * @param cls          the cls
   * @param emailAddress the email address
   * @param url          the url
   * @return The requested configuration settings. (TSettings The type of the
   * settings to retrieve)
   * @throws Exception the exception
   */
  private <TSettings extends ConfigurationSettingsBase>
  TSettings getLegacyUserSettingsAtUrl(
      Class<TSettings> cls, String emailAddress, URI url)
      throws Exception {
    this
        .traceMessage(TraceFlags.AutodiscoverConfiguration, String
            .format("Trying to call Autodiscover for %s on %s.",
                emailAddress, url));

    TSettings settings = cls.newInstance();

    HttpWebRequest request = null;
    try {
      request = this.prepareHttpWebRequestForUrl(url);

      this.traceHttpRequestHeaders(
          TraceFlags.AutodiscoverRequestHttpHeaders,
          request);
      OutputStream urlOutStream = request.getOutputStream();

      PrintWriter writer = new PrintWriter(urlOutStream);
      this.writeLegacyAutodiscoverRequest(emailAddress, settings, writer);

      writer.flush();
      urlOutStream.flush();
      urlOutStream.close();

      request.executeRequest();
      request.getResponseCode();
      URI redirectUrl;
      OutParam<URI> outParam = new OutParam<URI>();
      if (this.tryGetRedirectionResponse(request, outParam)) {
        redirectUrl = outParam.getParam();
        settings.makeRedirectionResponse(redirectUrl);
        return settings;
      }
      InputStream serviceResponseStream = request.getInputStream();

      EwsXmlReader reader = new EwsXmlReader(serviceResponseStream);
      reader.read(new XmlNodeType(XmlNodeType.START_DOCUMENT));
      settings.loadFromXml(reader);

      serviceResponseStream.close();
    } finally {
      if (request != null) {
        try {
          request.close();
        } catch (Exception ignored) {
        }
      }
    }

    return settings;
  }

  /**
   * Writes the autodiscover request.
   *
   * @param emailAddress the email address
   * @param settings     the settings
   * @param writer       the writer
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   */
  private void writeLegacyAutodiscoverRequest(String emailAddress,
      ConfigurationSettingsBase settings, PrintWriter writer) {
    writer.write(String.format("<Autodiscover xmlns=\"%s\">",
        AutodiscoverRequestNamespace));
    writer.write("<Request>");
    writer.write(String.format("<EMailAddress>%s</EMailAddress>",
        emailAddress));
    writer.write(String.format(
        "<AcceptableResponseSchema>%s</AcceptableResponseSchema>",
        settings.getNamespace()));
    writer.write("</Request>");
    writer.write("</Autodiscover>");
  }

  /**
   * Gets a redirection URL to an SSL-enabled Autodiscover service from the
   * standard non-SSL Autodiscover URL.
   *
   * @param domainName the domain name
   * @return A valid SSL-enabled redirection URL. (May be null).
   * @throws microsoft.exchange.webservices.data.EWSHttpException the eWS http exception
   * @throws javax.xml.stream.XMLStreamException                  the xML stream exception
   * @throws java.io.IOException                                  Signals that an I/O exception has occurred.
   * @throws ServiceLocalException                                the service local exception
   * @throws java.net.URISyntaxException                          the uRI syntax exception
   */
  private URI getRedirectUrl(String domainName)
      throws EWSHttpException, XMLStreamException, IOException, ServiceLocalException, URISyntaxException {
    String url = String.format(AutodiscoverLegacyHttpUrl, "autodiscover." + domainName);

    traceMessage(TraceFlags.AutodiscoverConfiguration,
        String.format("Trying to get Autodiscover redirection URL from %s.", url));

    HttpWebRequest request = null;

    try {
      request = new HttpClientWebRequest(getHttpClient(), httpContext);

      try {
        request.setUrl(URI.create(url).toURL());
      } catch (MalformedURLException e) {
        String strErr = String.format("Incorrect format : %s", url);
        throw new ServiceLocalException(strErr);
      }

      request.setRequestMethod("GET");
      request.setAllowAutoRedirect(false);
      request.setTimeout(timeout);

      // Do NOT allow authentication as this single request will be made over plain HTTP.
      request.setAllowAuthentication(false);

      prepareCredentials(request);

      request.prepareConnection();
      try {
        request.executeRequest();
      } catch (IOException e) {
        traceMessage(TraceFlags.AutodiscoverConfiguration, "No Autodiscover redirection URL was returned.");
        return null;
      }

      OutParam<URI> outParam = new OutParam<URI>();
      if (tryGetRedirectionResponse(request, outParam)) {
        return outParam.getParam();
      }
    } finally {
      if (request != null) {
        try {
          request.close();
        } catch (Exception ignored) {
        }
      }
    }

    traceMessage(TraceFlags.AutodiscoverConfiguration, "No Autodiscover redirection URL was returned.");
    return null;
  }

  /**
   * Tries the get redirection response.
   *
   * @param request     the request
   * @param redirectUrl The redirect URL.
   * @return True if a valid redirection URL was found.
   * @throws javax.xml.stream.XMLStreamException                  the xML stream exception
   * @throws java.io.IOException                                  Signals that an I/O exception has occurred.
   * @throws microsoft.exchange.webservices.data.EWSHttpException the eWS http exception
   */
  private boolean tryGetRedirectionResponse(HttpWebRequest request,
      OutParam<URI> redirectUrl) throws XMLStreamException, IOException,
      EWSHttpException {
    if (AutodiscoverRequest.isRedirectionResponse(request)) {
      // Get the redirect location and verify that it's valid.
      String location = request.getResponseHeaderField("Location");

      if (location != null && !location.isEmpty()) {
        try {
          redirectUrl.setParam(new URI(location));

          // Check if URL is SSL and that the path matches.
          if ((redirectUrl.getParam().getScheme().toLowerCase()
              .equals("https")) &&
              (redirectUrl.getParam().getPath()
                  .equalsIgnoreCase(
                      AutodiscoverLegacyPath))) {
            this.traceMessage(TraceFlags.AutodiscoverConfiguration,
                String.format("Redirection URL found: '%s'",
                    redirectUrl.getParam().toString()));

            return true;
          }
        } catch (URISyntaxException ex) {
          this
              .traceMessage(
                  TraceFlags.AutodiscoverConfiguration,
                  String
                      .format(
                          "Invalid redirection URL " +
                              "was returned: '%s'",
                          location));
          return false;
        }
      }
    }
    return false;
  }

  /**
   * Calls the legacy Autodiscover service to retrieve configuration settings.
   *
   * @param <TSettings>  the generic type
   * @param cls          the cls
   * @param emailAddress The email address to retrieve configuration settings for.
   * @return The requested configuration settings.
   * @throws Exception the exception
   */
  private <TSettings extends ConfigurationSettingsBase>
  TSettings getLegacyUserSettings(Class<TSettings> cls, String emailAddress) throws Exception {
    // If Url is specified, call service directly.
    if (this.url != null) {
      // this.Uri is intended for Autodiscover SOAP service, convert to Legacy endpoint URL.
      URI autodiscoverUrl = new URI(this.url.toString() + AutodiscoverLegacyPath);
      return this.getLegacyUserSettingsAtUrl(cls, emailAddress, autodiscoverUrl);
    }

    // If Domain is specified, figure out the endpoint Url and call service.
    else if (this.domain != null && !this.domain.isEmpty()) {
      URI autodiscoverUrl = new URI(String.format(AutodiscoverLegacyHttpsUrl, this.domain));
      return this.getLegacyUserSettingsAtUrl(cls,
          emailAddress, autodiscoverUrl);
    } else {
      // No Url or Domain specified, need to figure out which endpoint to use.
      int currentHop = 1;
      OutParam<Integer> outParam = new OutParam<Integer>();
      outParam.setParam(currentHop);
      return this.internalGetLegacyUserSettings(cls, emailAddress, outParam);
    }
  }

  /**
   * Calls the Autodiscover service to retrieve configuration settings.
   *
   * @param <TSettings>  the generic type
   * @param cls          the cls
   * @param emailAddress The email address to retrieve configuration settings for.
   * @param currentHop   Current number of redirection urls/addresses attempted so far.
   * @return The requested configuration settings.
   * @throws Exception the exception
   */
  private <TSettings extends ConfigurationSettingsBase>
  TSettings internalGetLegacyUserSettings(
      Class<TSettings> cls,
      String emailAddress,
      OutParam<Integer> currentHop)
      throws Exception {
    String domainName = EwsUtilities.domainFromEmailAddress(emailAddress);

    List<URI> urls = this.getAutodiscoverServiceUrls(domainName);
    if (urls.size() == 0) {
      throw new ServiceValidationException(
          Strings.AutodiscoverServiceRequestRequiresDomainOrUrl);
    }

    int currentUrlIndex = 0;

    TSettings settings;

    do {
      URI autodiscoverUrl = urls.get(currentUrlIndex);

      try {
        settings = this.getLegacyUserSettingsAtUrl(cls,
            emailAddress, autodiscoverUrl);

        switch (settings.getResponseType()) {
          case Success:
            this.url = autodiscoverUrl;
            return settings;
          case RedirectUrl:
            if (currentHop.getParam() < AutodiscoverMaxRedirections) {
              currentHop.setParam(currentHop.getParam() + 1);

              this
                  .traceMessage(
                      TraceFlags.AutodiscoverResponse,
                      String
                          .format("Autodiscover service returned redirection URL '%s'.",
                              settings
                                  .getRedirectTarget()));

              urls.set(currentUrlIndex, new URI(
                  settings.getRedirectTarget()));

              break;
            } else {
              throw new AutodiscoverLocalException(
                  Strings.MaximumRedirectionHopsExceeded);
            }
          case RedirectAddress:
            if (currentHop.getParam() < AutodiscoverMaxRedirections) {
              currentHop.setParam(currentHop.getParam() + 1);

              this
                  .traceMessage(
                      TraceFlags.AutodiscoverResponse, String.format(
                          "Autodiscover service returned redirection email address '%s'.",
                              settings
                                  .getRedirectTarget()));
              return this.internalGetLegacyUserSettings(cls,
                  settings.getRedirectTarget(),
                  currentHop);
            } else {
              throw new AutodiscoverLocalException(
                  Strings.MaximumRedirectionHopsExceeded);
            }
          case Error:
            throw new AutodiscoverRemoteException(Strings.AutodiscoverError, settings.getError());
          default:
            EwsUtilities
                .EwsAssert(false,
                    "Autodiscover.GetConfigurationSettings",
                    "An unexpected error has occured. " +
                        "This code path should never be reached.");
            break;
        }
      } catch (XMLStreamException ex) {
        this.traceMessage(TraceFlags.AutodiscoverConfiguration, String
            .format("%s failed: XML parsing error: %s", url, ex
                .getMessage()));

        // The content at the URL wasn't a valid response, let's try the
        // next.
        currentUrlIndex++;
      } catch (IOException ex) {
        this.traceMessage(
            TraceFlags.AutodiscoverConfiguration,
            String.format("%s failed: I/O error: %s",
                url, ex.getMessage()));

        // The content at the URL wasn't a valid response, let's try the next.
        currentUrlIndex++;
      } catch (Exception ex) {
        this.traceMessage(TraceFlags.AutodiscoverConfiguration,
            String.format("%s failed: %s (%s)", url, ex
                .getClass().getName(), ex.getMessage()));

        // The url did not work, let's try the next.
        currentUrlIndex++;
      }
    } while (currentUrlIndex < urls.size());

    // If we got this far it's because none of the URLs we tried have
    // worked. As a next-to-last chance, use GetRedirectUrl to
    // try to get a redirection URL using an HTTP GET on a non-SSL
    // Autodiscover endpoint. If successful, use this
    // redirection URL to get the configuration settings for this email
    // address. (This will be a common scenario for
    // DataCenter deployments).
    URI redirectionUrl = this.getRedirectUrl(domainName);
    OutParam<TSettings> outParam = new OutParam<TSettings>();
    if ((redirectionUrl != null)
        && this.tryLastChanceHostRedirection(cls, emailAddress,
        redirectionUrl, outParam)) {
      settings = outParam.getParam();
      return settings;
    } else {
      // Getting a redirection URL from an HTTP GET failed too. As a last
      // chance, try to get an appropriate SRV Record
      // using DnsQuery. If successful, use this redirection URL to get
      // the configuration settings for this email address.
      redirectionUrl = this.getRedirectionUrlFromDnsSrvRecord(domainName);
      if ((redirectionUrl != null)
          && this.tryLastChanceHostRedirection(cls, emailAddress,
          redirectionUrl, outParam)) {
        return outParam.getParam();
      }

      throw new AutodiscoverLocalException(Strings.AutodiscoverCouldNotBeLocated);
    }
  }

  /**
   * Get an autodiscover SRV record in DNS and construct autodiscover URL.
   *
   * @param domainName Name of the domain.
   * @return Autodiscover URL (may be null if lookup failed)
   * @throws Exception the exception
   */
  private URI getRedirectionUrlFromDnsSrvRecord(String domainName)
      throws Exception {

    this
        .traceMessage(
            TraceFlags.AutodiscoverConfiguration,
            String
                .format(
                    "Trying to get Autodiscover host " +
                        "from DNS SRV record for %s.",
                    domainName));

    String hostname = this.dnsClient
        .findAutodiscoverHostFromSrv(domainName);
    if (hostname != null && !hostname.isEmpty()) {
      this
          .traceMessage(TraceFlags.AutodiscoverConfiguration,
              String.format(
                  "Autodiscover host %s was returned.",
                  hostname));

      return new URI(String.format(AutodiscoverLegacyHttpsUrl,
          hostname));
    } else {
      this.traceMessage(TraceFlags.AutodiscoverConfiguration,
          "No matching Autodiscover DNS SRV records were found.");

      return null;
    }
  }

  /**
   * Tries to get Autodiscover settings using redirection Url.
   *
   * @param <TSettings>    the generic type
   * @param cls            the cls
   * @param emailAddress   The email address.
   * @param redirectionUrl Redirection Url.
   * @param settings       The settings.
   * @return boolean The boolean.
   * @throws AutodiscoverLocalException  the autodiscover local exception
   * @throws AutodiscoverRemoteException the autodiscover remote exception
   * @throws Exception                   the exception
   */
  private <TSettings extends ConfigurationSettingsBase> boolean
  tryLastChanceHostRedirection(
      Class<TSettings> cls, String emailAddress, URI redirectionUrl,
      OutParam<TSettings> settings) throws AutodiscoverLocalException,
      AutodiscoverRemoteException, Exception {
    // Bug 60274: Performing a non-SSL HTTP GET to retrieve a redirection
    // URL is potentially unsafe. We allow the caller
    // to specify delegate to be called to determine whether we are allowed
    // to use the redirection URL.
    if (this
        .callRedirectionUrlValidationCallback(
            redirectionUrl.toString())) {
      for (int currentHop = 0; currentHop < AutodiscoverService.AutodiscoverMaxRedirections; currentHop++) {
        try {
          settings.setParam(this.getLegacyUserSettingsAtUrl(cls,
              emailAddress, redirectionUrl));

          switch (settings.getParam().getResponseType()) {
            case Success:
              return true;
            case Error:
              throw new AutodiscoverRemoteException(
                  Strings.AutodiscoverError, settings.getParam()
                  .getError());
            case RedirectAddress:
              OutParam<Integer> outParam = new OutParam<Integer>();
              outParam.setParam(currentHop);
              settings.setParam(
                  this.internalGetLegacyUserSettings(cls,
                      settings.getParam().getRedirectTarget(),
                      outParam));
              return true;
            case RedirectUrl:
              try {
                redirectionUrl = new URI(settings.getParam()
                    .getRedirectTarget());
              } catch (URISyntaxException ex) {
                this.traceMessage(TraceFlags.
                                      AutodiscoverConfiguration, String
                                      .format("Service returned invalid redirection URL %s",
                                              settings.getParam().getRedirectTarget()));
                return false;
              }
              break;
            default:
              String failureMessage = String.format(
                  "Autodiscover call at %s failed with error %s, target %s",
                  redirectionUrl,
                  settings.getParam().getResponseType(),
                  settings.getParam().getRedirectTarget());
              this.traceMessage(
                  TraceFlags.AutodiscoverConfiguration, failureMessage);

              return false;
          }
        } catch (XMLStreamException ex) {
          // If the response is malformed, it wasn't a valid
          // Autodiscover endpoint.
          this
              .traceMessage(TraceFlags.AutodiscoverConfiguration,
                  String.format(
                      "%s failed: XML parsing error: %s",
                      redirectionUrl.toString(), ex
                          .getMessage()));
          return false;
        } catch (IOException ex) {
          this.traceMessage(
              TraceFlags.AutodiscoverConfiguration,
              String.format("%s failed: I/O error: %s",
                  redirectionUrl, ex.getMessage()));
          return false;
        } catch (Exception ex) {
          this
              .traceMessage(
                  TraceFlags.AutodiscoverConfiguration,
                  String.format("%s failed: %s (%s)",
                      url, ex.getClass().getName(),
                      ex.getMessage()));
          return false;
        }
      }
    }

    return false;
  }

  /**
   * Gets user settings from Autodiscover legacy endpoint.
   *
   * @param emailAddress      The email address to use.
   * @param requestedSettings The requested settings.
   * @return GetUserSettingsResponse
   */
  private GetUserSettingsResponse internalGetLegacyUserSettings(String emailAddress,
      List<UserSettingName> requestedSettings) throws Exception {
    // Cannot call legacy Autodiscover service with WindowsLive and other WSSecurity-based credentials
    if (this.getCredentials() instanceof WSSecurityBasedCredentials) {
      throw new AutodiscoverLocalException(Strings.WLIDCredentialsCannotBeUsedWithLegacyAutodiscover);
    }

    OutlookConfigurationSettings settings = this.getLegacyUserSettings(
        OutlookConfigurationSettings.class,
        emailAddress);

    return settings.convertSettings(emailAddress, requestedSettings);
  }

  /**
   * Calls the SOAP Autodiscover service
   * for user settings for a single SMTP address.
   *
   * @param smtpAddress       SMTP address.
   * @param requestedSettings The requested settings.
   * @return GetUserSettingsResponse
   */
  private GetUserSettingsResponse internalGetSoapUserSettings(String smtpAddress,
      List<UserSettingName> requestedSettings) throws Exception {
    List<String> smtpAddresses = new ArrayList<String>();
    smtpAddresses.add(smtpAddress);

    for (int currentHop = 0; currentHop < AutodiscoverService.AutodiscoverMaxRedirections; currentHop++) {
      GetUserSettingsResponse response = this.getUserSettings(smtpAddresses,
          requestedSettings);

      switch (response.getErrorCode()) {
        case RedirectAddress:
          this.traceMessage(
              TraceFlags.AutodiscoverResponse,
              String.format("Autodiscover service returned redirection email address '%s'.",
                  response.getRedirectTarget()));

          smtpAddresses.clear();
          smtpAddresses.add(response.getRedirectTarget().
              toLowerCase());
          this.url = null;
          this.domain = null;
          break;

        case RedirectUrl:
          this.traceMessage(
              TraceFlags.AutodiscoverResponse,
              String.format("Autodiscover service returned redirection URL '%s'.",
                  response.getRedirectTarget()));

          this.url = this.getCredentials().adjustUrl(new URI(response.getRedirectTarget()));
          break;

        case NoError:
        default:
          return response;
      }
    }

    throw new AutodiscoverLocalException(
        Strings.AutodiscoverCouldNotBeLocated);
  }

  /**
   * Gets the user settings using Autodiscover SOAP service.
   *
   * @param smtpAddresses The SMTP addresses of the users.
   * @param settings      The settings.
   * @return GetUserSettingsResponseCollection Object.
   * @throws Exception the exception
   */
  private GetUserSettingsResponse getUserSettings(final List<String> smtpAddresses,
      List<UserSettingName> settings)
      throws Exception {
    EwsUtilities.validateParam(smtpAddresses, "smtpAddresses");
    EwsUtilities.validateParam(settings, "settings");

    return this.getSettings(smtpAddresses, settings);
  }

  /**
   * Gets user or domain settings using Autodiscover SOAP service.
   *
   * @param identities                       Either the domains or the SMTP addresses of the users.
   * @param settings                         The settings.
   * @return TGetSettingsResponse Collection.
   * @throws Exception the exception
   */
  private GetUserSettingsResponse getSettings(List<String> identities,
      List<UserSettingName> settings) throws Exception {
    GetUserSettingsResponseCollection response;

    // Autodiscover service only exists in E14 or later.
    if (this.getRequestedServerVersion().compareTo(
        MinimumRequestVersionForAutoDiscoverSoapService) < 0) {
      throw new ServiceVersionException(String.format(
          Strings.AutodiscoverServiceIncompatibleWithRequestVersion,
          MinimumRequestVersionForAutoDiscoverSoapService));
    }

    // If Url is specified, call service directly.
    if (this.url != null) {
      URI autodiscoverUrl = this.url;
      response = internalGetUserSettings(identities, settings, this.url);
      this.url = autodiscoverUrl;
      return response.getTResponseAtIndex(0);
    }
    // If Domain is specified, determine endpoint Url and call service.
    else if (!(this.domain == null || this.domain.isEmpty())) {
      URI autodiscoverUrl = this.getAutodiscoverEndpointUrl(this.domain);
      response = internalGetUserSettings(identities, settings, autodiscoverUrl);

      // If we got this far, response was successful, set Url.
      this.url = autodiscoverUrl;
      return response.getTResponseAtIndex(0);
    }
    // No Url or Domain specified, need to figure out which endpoint(s) to
    // try.
    else {
      URI autodiscoverUrl;

      String domainName = EwsUtilities.domainFromEmailAddress(identities.get(0));
      List<String> hosts = this.getAutodiscoverServiceHosts(domainName);
      if (hosts.size() == 0) {
        throw new ServiceValidationException(
            Strings.AutodiscoverServiceRequestRequiresDomainOrUrl);
      }

      for (String host : hosts) {
        OutParam<URI> outParams = new OutParam<URI>();
        if (this.tryGetAutodiscoverEndpointUrl(host, outParams)) {
          autodiscoverUrl = outParams.getParam();
          response = internalGetUserSettings(identities, settings, autodiscoverUrl);

          // If we got this far, the response was successful, set Url.
          this.url = autodiscoverUrl;

          return response.getTResponseAtIndex(0);
        }
      }

      // Next-to-last chance: try unauthenticated GET over HTTP to be
      // redirected to appropriate service endpoint.
      autodiscoverUrl = this.getRedirectUrl(domainName);
      OutParam<URI> outParamUrl = new OutParam<URI>();
      if ((autodiscoverUrl != null) &&
          this
              .callRedirectionUrlValidationCallback(
                  autodiscoverUrl.toString()) &&
          this.tryGetAutodiscoverEndpointUrl(autodiscoverUrl
              .getHost(), outParamUrl)) {
        autodiscoverUrl = outParamUrl.getParam();

        // Shortcut for O365, as autodiscover does not always work well there
        if(autodiscoverUrl.getHost().equals(o365AutodiscoverHost)) {
          return createO365UserSettingsResponse();
        }

        response = internalGetUserSettings(identities, settings, autodiscoverUrl);

        // If we got this far, the response was successful, set Url.
        this.url = autodiscoverUrl;

        return response.getTResponseAtIndex(0);
      }

      // Last Chance: try to read autodiscover SRV Record from DNS. If we
      // find one, use
      // the hostname returned to construct an Autodiscover endpoint URL.
      autodiscoverUrl = this
          .getRedirectionUrlFromDnsSrvRecord(domainName);
      if ((autodiscoverUrl != null) &&
          this
              .callRedirectionUrlValidationCallback(
                  autodiscoverUrl.toString()) &&
          this.tryGetAutodiscoverEndpointUrl(autodiscoverUrl
              .getHost(), outParamUrl)) {
        autodiscoverUrl = outParamUrl.getParam();
        response = internalGetUserSettings(identities, settings, autodiscoverUrl);

        // If we got this far, the response was successful, set Url.
        this.url = autodiscoverUrl;

        return response.getTResponseAtIndex(0);
      } else {
        throw new AutodiscoverLocalException(
            Strings.AutodiscoverCouldNotBeLocated);
      }
    }
  }

  /**
   * Gets settings for one or more users.
   *
   * @param smtpAddresses    The SMTP addresses of the users.
   * @param settings         The settings.
   * @param autodiscoverUrl  The autodiscover URL.
   * @return GetUserSettingsResponse collection.
   * @throws ServiceLocalException the service local exception
   * @throws Exception             the exception
   */
  private GetUserSettingsResponseCollection internalGetUserSettings(List<String> smtpAddresses,
      List<UserSettingName> settings, URI autodiscoverUrl) throws ServiceLocalException, Exception {
    // The response to GetUserSettings can be a redirection. Execute
    // GetUserSettings until we get back
    // a valid response or we've followed too many redirections.
    for (int currentHop = 0; currentHop < AutodiscoverService.AutodiscoverMaxRedirections; currentHop++) {
      GetUserSettingsRequest request = new GetUserSettingsRequest(this, autodiscoverUrl);
      request.setSmtpAddresses(smtpAddresses);
      request.setSettings(settings);
      GetUserSettingsResponseCollection response = request.execute();

      // Did we get redirected?
      if (response.getErrorCode() == AutodiscoverErrorCode.RedirectUrl
          && response.getRedirectionUrl() != null) {
        this.traceMessage(
            TraceFlags.AutodiscoverConfiguration,
            String.format("Request to %s returned redirection to %s",
                autodiscoverUrl.toString(), response.getRedirectionUrl()));

        autodiscoverUrl = response.getRedirectionUrl();
      } else {
        return response;
      }
    }

    this.traceMessage(TraceFlags.AutodiscoverConfiguration, String.format(
        "Maximum number of redirection hops %d exceeded",
        AutodiscoverMaxRedirections));

    throw new AutodiscoverLocalException(
        Strings.MaximumRedirectionHopsExceeded);
  }

  /**
   * Gets the autodiscover endpoint URL.
   *
   * @param host The host.
   * @return URI The URI.
   * @throws Exception the exception
   */
  private URI getAutodiscoverEndpointUrl(String host) throws Exception {
    URI autodiscoverUrl = null;
    OutParam<URI> outParam = new OutParam<URI>();
    if (this.tryGetAutodiscoverEndpointUrl(host, outParam)) {
      return autodiscoverUrl;
    } else {
      throw new AutodiscoverLocalException(
          Strings.NoSoapOrWsSecurityEndpointAvailable);
    }
  }

  /**
   * Tries the get Autodiscover Service endpoint URL.
   *
   * @param host The host.
   * @param url  the url
   * @return boolean The boolean.
   * @throws Exception the exception
   */
  private boolean tryGetAutodiscoverEndpointUrl(String host,
      OutParam<URI> url)
      throws Exception {
    EnumSet<AutodiscoverEndpoints> endpoints;
    OutParam<EnumSet<AutodiscoverEndpoints>> outParam =
        new OutParam<EnumSet<AutodiscoverEndpoints>>();
    if (this.tryGetEnabledEndpointsForHost(host, outParam)) {
      endpoints = outParam.getParam();
      url
          .setParam(new URI(String.format(AutodiscoverSoapHttpsUrl,
              host)));

      // Make sure that at least one of the non-legacy endpoints is
      // available.
      if ((!endpoints.contains(AutodiscoverEndpoints.Soap)) &&
          (!endpoints.contains(
              AutodiscoverEndpoints.WsSecurity))) {
        this
            .traceMessage(
                TraceFlags.AutodiscoverConfiguration,
                String
                    .format(
                        "No Autodiscover endpoints " +
                            "are available  for host %s",
                        host));

        return false;
      }

      return true;
    } else {
      this
          .traceMessage(
              TraceFlags.AutodiscoverConfiguration,
              String
                  .format(
                      "No Autodiscover endpoints " +
                          "are available for host %s",
                      host));

      return false;
    }
  }

  /**
   * Gets the list of autodiscover service URLs.
   *
   * @param domainName   Domain name.
   * @return List of Autodiscover URLs.
   * @throws java.net.URISyntaxException the URI Syntax exception
   */
  private List<URI> getAutodiscoverServiceUrls(String domainName)
      throws URISyntaxException {
    List<URI> urls = new ArrayList<URI>();

    // As a fallback, add autodiscover URLs base on the domain name.
    urls.add(new URI(String.format(AutodiscoverLegacyHttpsUrl,
                                   "autodiscover." + domainName)));
    urls.add(new URI(String.format(AutodiscoverLegacyHttpsUrl,
        domainName)));

    return urls;
  }

  /**
   * Gets the list of autodiscover service hosts.
   *
   * @param domainName Domain name.
   * @return List of hosts.
   * @throws java.net.URISyntaxException the uRI syntax exception
   * @throws ClassNotFoundException      the class not found exception
   */
  private List<String> getAutodiscoverServiceHosts(String domainName) throws URISyntaxException {

    List<URI> urls = this.getAutodiscoverServiceUrls(domainName);
    List<String> lst = new ArrayList<String>();
    for (URI url : urls) {
      lst.add(url.getHost());
    }
    return lst;
  }

  /**
   * Gets the enabled autodiscover endpoints on a specific host.
   *
   * @param host      The host.
   * @param endpoints Endpoints found for host.
   * @return Flags indicating which endpoints are enabled.
   * @throws Exception the exception
   */
  private boolean tryGetEnabledEndpointsForHost(String host,
      OutParam<EnumSet<AutodiscoverEndpoints>> endpoints) throws Exception {
    this.traceMessage(TraceFlags.AutodiscoverConfiguration, String.format(
        "Determining which endpoints are enabled for host %s", host));

    // We may get redirected to another host. And therefore need to limit the number of redirections we'll
    // tolerate.
    for (int currentHop = 0; currentHop < AutodiscoverMaxRedirections; currentHop++) {
      URI autoDiscoverUrl = new URI(String.format(AutodiscoverLegacyHttpsUrl, host));

      endpoints.setParam(EnumSet.of(AutodiscoverEndpoints.None));

      HttpWebRequest request = null;
      try {
        request = new HttpClientWebRequest(getHttpClient(), httpContext);

        try {
          request.setUrl(autoDiscoverUrl.toURL());
        } catch (MalformedURLException e) {
          String strErr = String.format("Incorrect format : %s", url);
          throw new ServiceLocalException(strErr);
        }

        request.setRequestMethod("GET");
        request.setAllowAutoRedirect(false);
        request.setPreAuthenticate(false);
        request.setUseDefaultCredentials(this.getUseDefaultCredentials());
        request.setTimeout(timeout);

        prepareCredentials(request);

        request.prepareConnection();
        try {
          request.executeRequest();
        } catch (IOException e) {
          return false;
        }

        OutParam<URI> outParam = new OutParam<URI>();
        if (this.tryGetRedirectionResponse(request, outParam)) {
          URI redirectUrl = outParam.getParam();
          this.traceMessage(TraceFlags.AutodiscoverConfiguration,
              String.format("Host returned redirection to host '%s'", redirectUrl.getHost()));

          host = redirectUrl.getHost();
        } else {
          endpoints.setParam(this.getEndpointsFromHttpWebResponse(request));

          this.traceMessage(TraceFlags.AutodiscoverConfiguration,
              String.format("Host returned enabled endpoint flags: %s", endpoints.getParam().toString()));

          return true;
        }
      } finally {
        if (request != null) {
          try {
            request.close();
          } catch (Exception e) {
            // Connection can't be closed. We'll ignore this...
          }
        }
      }
    }

    this.traceMessage(TraceFlags.AutodiscoverConfiguration,
        String.format("Maximum number of redirection hops %d exceeded", AutodiscoverMaxRedirections));

    throw new AutodiscoverLocalException(Strings.MaximumRedirectionHopsExceeded);
  }

  /**
   * Gets the endpoints from HTTP web response.
   *
   * @param request the request
   * @return Endpoints enabled.
   * @throws microsoft.exchange.webservices.data.EWSHttpException the eWS http exception
   */
  private EnumSet<AutodiscoverEndpoints> getEndpointsFromHttpWebResponse(
      HttpWebRequest request) throws EWSHttpException {
    EnumSet<AutodiscoverEndpoints> endpoints = EnumSet
        .noneOf(AutodiscoverEndpoints.class);
    endpoints.add(AutodiscoverEndpoints.Legacy);

    if (!(request.getResponseHeaders().get(
        AutodiscoverSoapEnabledHeaderName) == null || request
        .getResponseHeaders().get(AutodiscoverSoapEnabledHeaderName)
        .isEmpty())) {
      endpoints.add(AutodiscoverEndpoints.Soap);
    }
    if (!(request.getResponseHeaders().get(
        AutodiscoverWsSecurityEnabledHeaderName) == null || request
        .getResponseHeaders().get(
            AutodiscoverWsSecurityEnabledHeaderName).isEmpty())) {
      endpoints.add(AutodiscoverEndpoints.WsSecurity);
    }

    return endpoints;
  }

  /**
   * Traces the response.
   *
   * @param request      the request
   * @param memoryStream the memory stream
   * @throws javax.xml.stream.XMLStreamException                  the xML stream exception
   * @throws java.io.IOException                                  Signals that an I/O exception has occurred.
   * @throws microsoft.exchange.webservices.data.EWSHttpException the eWS http exception
   */
  void traceResponse(HttpWebRequest request, ByteArrayOutputStream memoryStream) throws XMLStreamException,
      IOException, EWSHttpException {
    this.processHttpResponseHeaders(
        TraceFlags.AutodiscoverResponseHttpHeaders, request);
    String contentType = request.getResponseContentType();
    if (!(contentType == null || contentType.isEmpty())) {
      contentType = contentType.toLowerCase();
      if (contentType.toLowerCase().startsWith("text/") ||
          contentType.toLowerCase().
              startsWith("application/soap")) {
        this.traceXml(TraceFlags.AutodiscoverResponse, memoryStream);
      } else {
        this.traceMessage(TraceFlags.AutodiscoverResponse,
            "Non-textual response");
      }
    }
  }

  /**
   * Creates an HttpWebRequest instance and initializes it with the
   * appropriate parameters, based on the configuration of this service
   * object.
   *
   * @param url The URL that the HttpWebRequest should target.
   * @return HttpWebRequest The HttpWebRequest.
   * @throws ServiceLocalException       the service local exception
   * @throws java.net.URISyntaxException the uRI syntax exception
   */
  HttpWebRequest prepareHttpWebRequestForUrl(URI url)
      throws ServiceLocalException, URISyntaxException {
    return this.prepareHttpWebRequestForUrl(url, false,false);
  }

  /**
   * Calls the redirection URL validation callback. If the redirection URL
   * validation callback is null, use the default callback which does not
   * allow following any redirections.
   *
   * @param redirectionUrl The redirection URL.
   * @return True if redirection should be followed.
   * @throws AutodiscoverLocalException the autodiscover local exception
   */
  private boolean callRedirectionUrlValidationCallback(String redirectionUrl)
      throws AutodiscoverLocalException {
    if (this.redirectionUrlValidationCallback != null) {
      return redirectionUrlValidationCallback.autodiscoverRedirectionUrlValidationCallback(redirectionUrl);
    }

    return false;
  }

  /**
   * Processes an HTTP error response.
   *
   * @param httpWebResponse The HTTP web response.
   * @throws Exception the exception
   */
  @Override
  protected void processHttpErrorResponse(HttpWebRequest httpWebResponse,
      Exception webException) throws Exception {
    this.internalProcessHttpErrorResponse(
        httpWebResponse,
        webException,
        TraceFlags.AutodiscoverResponseHttpHeaders,
        TraceFlags.AutodiscoverResponse);
  }

  /**
   * Initializes a new instance of the AutodiscoverService class.
   *
   * @param service                The other service.
   * @param requestedServerVersion The requested server version.
   */
  AutodiscoverService(ExchangeServiceBase service, ExchangeVersion requestedServerVersion) {
    super(service, requestedServerVersion);
    this.dnsClient = new AutodiscoverDnsClient(this);
  }

  /**
   * Retrieves the specified settings for single SMTP address.
   *
   * @param userSmtpAddress  The SMTP addresses of the user.
   * @param userSettingNames The user setting names.
   * @return A UserResponse object containing the requested settings for the
   * specified user.
   * @throws Exception the exception
   *                   <p/>
   *                   This method handles will run the entire Autodiscover "discovery"
   *                   algorithm and will follow address and URL redirections.
   */
  GetUserSettingsResponse getUserSettings(String userSmtpAddress, UserSettingName... userSettingNames) throws Exception {
    List<UserSettingName> requestedSettings = new ArrayList<UserSettingName>();
    requestedSettings.addAll(Arrays.asList(userSettingNames));

    if (userSmtpAddress == null || userSmtpAddress.isEmpty()) {
      throw new ServiceValidationException(
          Strings.InvalidAutodiscoverSmtpAddress);
    }

    if (requestedSettings.size() == 0) {
      throw new ServiceValidationException(
          Strings.InvalidAutodiscoverSettingsCount);
    }

    if (this.getRequestedServerVersion().compareTo(MinimumRequestVersionForAutoDiscoverSoapService) < 0) {
      return this.internalGetLegacyUserSettings(userSmtpAddress,
          requestedSettings);
    } else {
      return this.internalGetSoapUserSettings(userSmtpAddress,
          requestedSettings);
    }

  }

  /**
   * Gets the domain this service is bound to. When this property is
   * set, the domain
   * <p/>
   * name is used to automatically determine the Autodiscover service URL.
   *
   * @return the domain
   */
  public String getDomain() {
    return this.domain;
  }

  /**
   * Sets the domain this service is bound to. When this property is
   * set, the domain
   * name is used to automatically determine the Autodiscover service URL.
   *
   * @param value the new domain
   * @throws microsoft.exchange.webservices.data.ArgumentException
   */
  public void setDomain(String value) throws ArgumentException {
    EwsUtilities.validateDomainNameAllowNull(value, "Domain");

    // If Domain property is set to non-null value, Url property is nulled.
    if (value != null) {
      this.url = null;
    }
    this.domain = value;
  }

  /**
   * Gets the url this service is bound to.
   *
   * @return the url
   */
  public URI getUrl() {
    return this.url;
  }

  /**
   * Sets the url this service is bound to.
   *
   * @param value the new url
   */
  public void setUrl(URI value) {
    // If Url property is set to non-null value, Domain property is set to
    // host portion of Url.
    if (value != null) {
      this.domain = value.getHost();
    }
    this.url = value;
  }

  /**
   * Sets the redirection url validation callback.
   *
   * @param value the new redirection url validation callback
   */
  public void setRedirectionUrlValidationCallback(
      IAutodiscoverRedirectionUrl value) {
    this.redirectionUrlValidationCallback = value;
  }

  /**
   * Gets the dns server address.
   *
   * @return the dns server address
   */
  protected String getDnsServerAddress() {
    return this.dnsServerAddress;
  }

  /**
   * Sets the dns server address.
   *
   * @param value the new dns server address
   */
  protected void setDnsServerAddress(String value) {
    this.dnsServerAddress = value;
  }

  /**
   * @return Dummy response containing the O365 EWS url.
   */
  private GetUserSettingsResponse createO365UserSettingsResponse() {
    GetUserSettingsResponse response = new GetUserSettingsResponse();

    response.setErrorCode(AutodiscoverErrorCode.NoError);
    response.getSettings().put(UserSettingName.ExternalEwsUrl, o365EwsUrl);
    response.getSettings().put(UserSettingName.InternalEwsUrl, o365EwsUrl);

    return response;
  }
}
