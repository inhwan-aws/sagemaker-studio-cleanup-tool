import sys
import boto3
import time
import argparse

class SageMakerStudioCleanUpTool:
    verbose = False
    wait = False

    def __init__(self):
        self.session = boto3.Session(region_name='us-east-2')
        self.sagemaker = self.session.client(service_name='sagemaker')        

    def _getApps(self, domainId):
        if self.verbose == True:
            print("list_apps - DomainIdEquals : " + domainId)

        apps = []
        try:
            response = self.sagemaker.list_apps(DomainIdEquals=domainId)

            if self.verbose == True:
                print(response)

            apps = response['Apps']
        except Exception as e:
            print(e)
        
        return apps

    def _deleteApp(self, domainId, userProfileName, appType, appName):
        print("delete App - DomainId : " + domainId + ", UserProfileName : " + userProfileName + ", AppType : " + appType + ", AppName : " + appName)

        try:
            response = self.sagemaker.delete_app(DomainId=domainId, UserProfileName=userProfileName, AppType=appType, AppName=appName)

            if self.verbose == True:
                print(response)
        except Exception as e:
            print(e)
    
    def _deleteApps(self, domainId):       
        while(True):
            apps = self._getApps(domainId)

            if len(apps) > 0:
                allDeleted = True

                for app in apps:
                    if app['Status'] != 'Deleted':
                        self._deleteApp(app['DomainId'], app['UserProfileName'], app['AppType'], app['AppName'])
                        allDeleted = False
    
                if allDeleted == True and self.wait == False:
                    break

                print("Waiting to be deleted all apps of DomainId : " + domainId)
                time.sleep(60)
            else:
                print("No app to be deleted.")
                break
            
    def _getUserProfiles(self, domainId):
        if self.verbose == True:
            print("list_user_profiles - DomainIdEquals : " + domainId)

        userProfiles = []
        try:
            response = self.sagemaker.list_user_profiles(DomainIdEquals=domainId)

            if self.verbose == True:
                print(response)

            userProfiles = response['UserProfiles']
        except Exception as e:
            print(e)

        return userProfiles

    def _deleteUserProfile(self, domainId, userProfileName):
        print("delete UserProfile - DomainId : " + domainId + ", UserProfileName : " + userProfileName)

        try:
            response = self.sagemaker.delete_user_profile(DomainId=domainId, UserProfileName=userProfileName)

            if self.verbose == True:
                print(response)
        except Exception as e:
            print(e)

    def _deleteUserProfiles(self, domainId):
        while(True):
            userProfiles = self._getUserProfiles(domainId)

            if len(userProfiles) > 0:
                allDeleted = True

                for userProfile in userProfiles:
                    if userProfile['Status'] != 'Deleted':
                        self._deleteUserProfile(userProfile['DomainId'], userProfile['UserProfileName'])
                        allDeleted = False
    
                if allDeleted == True and self.wait == False:
                    break
    
                print("Waiting to be deleted all user profiles of DomainId : " + domainId)
                time.sleep(60)
            else:
                print("No user profile to be deleted.")
                break

    def _getDomains(self):
        if self.verbose == True:
            print("list_domains")

        domains = []
        try:
            response = self.sagemaker.list_domains()

            if self.verbose == True:
                print(response)

            domains = response['Domains']
        except Exception as e:
            print(e)

        return domains

    def deleteDomain(self, domainId):
        self._deleteApps(domainId)

        self._deleteUserProfiles(domainId)

        print("delete Domain - DomainId : " + domainId)

        try:
            response = self.sagemaker.delete_domain(DomainId=domainId)

            if self.verbose == True:
                print(response)
        except Exception as e:
            print(e)

    def deleteDomains(self):
        while(True):
            domains = self._getDomains()

            if len(domains) > 0:
                allDeleted = True

                for domain in domains:
                    if domain['Status'] != 'Deleted':
                        self.deleteDomain(domain['DomainId'])
                        allDeleted = False
    
                if allDeleted == True and self.wait == False:
                    break
    
                print("Waiting to be deleted all domains")
                time.sleep(60)
            else:
                print("No domain to be deleted.")
                break


def _parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--domain-id', type=str, nargs='?', default='ALL')        
    parser.add_argument('--verbose', type=bool, nargs='?', default=False)
    parser.add_argument('--wait', type=bool, nargs='?', default=False)        

    args, _ = parser.parse_known_args()

    return args

if __name__ == "__main__":
    args = _parse_args()

    cleanUpTool = SageMakerStudioCleanUpTool()
    cleanUpTool.verbose = args.verbose
    cleanUpTool.wait = args.wait

    if args.domain_id == "ALL":
        cleanUpTool.deleteDomains()
    else:
        cleanUpTool.deleteDomain(args.domain_id)