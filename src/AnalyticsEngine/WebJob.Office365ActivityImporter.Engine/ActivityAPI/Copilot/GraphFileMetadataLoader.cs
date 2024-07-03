﻿using DataUtils;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using System;
using System.Threading.Tasks;
using WebJob.Office365ActivityImporter.Engine.ActivityAPI.Copilot;

namespace ActivityImporter.Engine.ActivityAPI.Copilot
{
    /// <summary>
    /// Populates file metadata from Graph API
    /// </summary>
    public class GraphFileMetadataLoader : ICopilotMetadataLoader
    {
        private readonly GraphServiceClient _graphServiceClient;
        private readonly SiteGraphCache _siteGraphCache;
        private readonly UserGraphCache _userGraphCache;
        private readonly ILogger _logger;

        public GraphFileMetadataLoader(GraphServiceClient graphServiceClient, ILogger logger)
        {
            _graphServiceClient = graphServiceClient;
            _logger = logger;
            _siteGraphCache = new SiteGraphCache(graphServiceClient);
            _userGraphCache = new UserGraphCache(graphServiceClient);
        }

        public async Task<MeetingMetadata> GetMeetingInfo(string meetingId, string userGuid)
        {
            // Requires OnlineMeetings.Read.All and https://learn.microsoft.com/en-us/graph/cloud-communication-online-meeting-application-access-policy#configure-application-access-policy
            try
            {
                var meeting = await _graphServiceClient.Users[userGuid].OnlineMeetings[meetingId].Request().GetAsync();

                return new MeetingMetadata(meeting);
            }
            catch (ServiceException ex)
            {
                _logger.LogWarning(ex, "Error getting meeting info for meetingId {meetingId}", meetingId);
                return null;
            }
        }

        // Example: https://m365cp123890-my.sharepoint.com/personal/sambetts_m365cp123890_onmicrosoft_com/_layouts/15/Doc.aspx?sourcedoc=%7B0D86F64F-8435-430C-8979-FF46C00F7ACB%7D&file=Presentation.pptx&action=edit&mobileredirect=true
        public async Task<SpoDocumentFileInfo> GetSpoFileInfo(string copilotDocContextId, string eventUpn)
        {
            var siteUrl = StringUtils.GetSiteUrl(copilotDocContextId);
            if (siteUrl == null) return null;

            Drive drive = null;
            if (StringUtils.IsMySiteUrl(siteUrl))
            {
                drive = await GetSpoInfoFromMySiteUrl(eventUpn);
            }
            else
            {
                drive = await GetSpoInfoFromSiteUrl(siteUrl);
            }
            if (drive == null)
            {
                return null;
            }

            // Get site ID from url
            // https://learn.microsoft.com/en-us/graph/api/drive-get?view=graph-rest-beta&tabs=http
            var spSiteId = drive.SharePointIds?.SiteId;
            if (string.IsNullOrEmpty(spSiteId))
            {
                throw new ArgumentOutOfRangeException("SharePointIds.SiteId");
            }
            var spListId = drive.SharePointIds?.ListId;
            if (string.IsNullOrEmpty(spListId))
            {
                throw new ArgumentOutOfRangeException("SharePointIds.ListId");
            }
            var driveItemId = StringUtils.GetDriveItemId(copilotDocContextId);

            ListItem item = null;
            var site = await _siteGraphCache.GetResourceOrNullIfNotExists(spSiteId);
            if (driveItemId != null)
            {
                try
                {
                    item = await _graphServiceClient.Sites[spSiteId].Lists[spListId].Items[driveItemId]
                        .Request().Expand("fields").GetAsync();
                }
                catch (ServiceException ex)
                {
                    _logger.LogWarning(ex, "Error getting file info for copilotDocContextId {copilotDocContextId}", copilotDocContextId);
                    return null;
                }

                return new SpoDocumentFileInfo(item, site);
            }
            else
            {
                // We might have a direct URL as the copilot context ID, so we need to search for the item in the list.
                // Example: https://contoso-my.sharepoint.com/personal/alex_contoso_onmicrosoft_com/Documents/MyDoc.docx
                try
                {
                    // Currently we can't filter by webUrl, so we have to get all items and filter client side
                    var listItems = await _graphServiceClient.Sites[spSiteId].Lists[spListId].Items
                        .Request().Select("id,webUrl").GetAsync();
                    if (listItems != null)
                    {
                        foreach (var i in listItems)
                        {
                            if (i.WebUrl == copilotDocContextId)
                            {
                                return new SpoDocumentFileInfo(i, site);
                            }
                        }
                    }
                }
                catch (ServiceException ex)
                {
                    _logger.LogWarning(ex, "Error getting items info for list {spListId} on site {siteUrl}", spListId, siteUrl);
                    return null;
                }

                _logger.LogWarning("No driveItemId found in copilotDocContextId {copilotDocContextId}", copilotDocContextId);
                return null;
            }
        }

        public async Task<string> GetUserIdFromUpn(string userPrincipalName)
        {
            var user = await _userGraphCache.GetResource(userPrincipalName);
            return user.Id ?? throw new Exception($"No user ID found on user in Graph by upn {userPrincipalName}");
        }

        private async Task<Drive> GetSpoInfoFromMySiteUrl(string eventUpn)
        {
            // Needs Files.Read.All
            try
            {
                return await _graphServiceClient.Users[eventUpn].Drive.Request().Select("SharePointIds").GetAsync() ?? throw new ArgumentOutOfRangeException(eventUpn);
            }
            catch (ServiceException ex)
            {
                _logger.LogWarning(ex, $"Error {ex.StatusCode} getting drive info for user {eventUpn}", eventUpn);
                return null;
            }
        }

        private async Task<Drive> GetSpoInfoFromSiteUrl(string siteUrl)
        {
            var siteAddress = StringUtils.GetHostAndSiteRelativeUrl(siteUrl);
            if (siteAddress == null)
            {
                // Possibly a Teams reference
                return null;
            }

            // Get drive ID from site ID
            Drive siteDrive = null;
            try
            {
                siteDrive = await _graphServiceClient.Sites[siteAddress].Drive.Request().Select("SharePointIds").GetAsync() ?? throw new ArgumentOutOfRangeException(siteAddress);
            }
            catch (ServiceException)
            {
                // We can't get the drive via the site address, for some reason. Most of the time we can, but sometimes it doesn't work...
                // Load just the site and then try getting the drive using the loaded site ID
            }

            if (siteDrive == null)
            {
                Site site = null;
                try
                {
                    site = await _graphServiceClient.Sites[siteAddress].Request().GetAsync() ?? throw new ArgumentOutOfRangeException(siteAddress);
                }
                catch (ServiceException ex)
                {
                    _logger.LogWarning(ex, "Error getting site info for site {siteUrl}", siteUrl);
                    return null;
                }
                if (site != null)
                {
                    try
                    {
                        // Try one more time using site ID
                        siteDrive = await _graphServiceClient.Sites[site.Id].Drive.Request().Select("SharePointIds").GetAsync() ?? throw new ArgumentOutOfRangeException(siteAddress);
                    }
                    catch (ServiceException)
                    {
                        // Ignore. Handle logging below
                    }

                    if (siteDrive == null)
                    {
                        // Site exists but no drive for some reason
                        _logger.LogWarning($"No drive found for site ID {site.Id}");
                        return null;
                    }
                    else
                    {
                        return siteDrive;
                    }
                }
                else
                {
                    // We can't find the site. Bug in the URL parsing?
                    _logger.LogError("No site found for site {siteUrl}", siteUrl);
                    return null;
                }
            }
            else
            {
                return siteDrive;
            }
        }
    }
}
