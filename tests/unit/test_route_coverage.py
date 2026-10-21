# tests/unit/test_route_coverage.py
"""Route-coverage guard: every registered API route must map to a test that covers it.

``EXPECTED`` maps ``"METHOD /path"`` (as registered on the app, HEAD/OPTIONS excluded)
to the test that exercises that route's handler:

* API routes point at a direct-call route test in ``tests/unit/api/`` (those tests call
  the exact handler function the route wraps);
* ``GET /`` and ``GET /favicon.ico`` are exercised over the full ASGI stack by
  ``tests/unit/test_main.py``.

If you add a route, add an entry here pointing at a test that actually hits the route's
handler - the guards below fail otherwise (and also fail on stale entries).
"""

import importlib
from unittest.mock import MagicMock, patch

import pytest
from fastapi.routing import APIRoute

from kronicle.main import KronicleApp


def _flatten_routes(routes):
    """Flatten every registered route (route objects or lazy include-wrappers) with a full path.

    FastAPI >= 0.153 wraps ``include_router`` calls in lazy objects that expose
    ``effective_candidates()`` instead of flattening eagerly, so both shapes are handled.
    """
    flattened = []
    for route in routes:
        if isinstance(route, APIRoute):
            flattened.append(route)
        elif hasattr(route, "effective_candidates"):
            for candidate in route.effective_candidates():
                if isinstance(candidate, APIRoute) or (
                    hasattr(candidate, "path") and hasattr(candidate, "methods") and not callable(candidate.path)
                ):
                    flattened.append(candidate)
                else:
                    flattened.extend(_flatten_routes([candidate]))
    return flattened


EXPECTED = {
    "DELETE /core/v1/channels/{channel_id}": "tests/unit/api/test_core_routes.py::TestDeleteCoreChannel::test_deletes_channel",
    "DELETE /core/v1/zones/{child_id}/parent": "tests/unit/api/test_core_routes.py::TestZoneHierarchyRoutes::test_detach_zone_parent",
    "DELETE /core/v1/zones/{zone_id}": "tests/unit/api/test_core_routes.py::TestDeleteZone::test_deletes_zone",
    "DELETE /rbac/v1/access-profiles/channels/{profile_id}": "tests/unit/api/test_rbac_routes.py::TestAccessProfileRoutes::test_delete_channel_access_profile",
    "DELETE /rbac/v1/access-profiles/rows/{profile_id}": "tests/unit/api/test_rbac_routes.py::TestAccessProfileRoutes::test_delete_row_access_profile",
    "DELETE /rbac/v1/access-profiles/zones/{profile_id}": "tests/unit/api/test_rbac_routes.py::TestAccessProfileRoutes::test_delete_zone_access_profile",
    "DELETE /rbac/v1/groups/{group_id}": "tests/unit/api/test_rbac_routes.py::TestGroupRoutes::test_delete_group",
    "DELETE /rbac/v1/groups/{group_id}/roles/{role_id}": "tests/unit/api/test_rbac_routes.py::TestGroupRoleRoutes::test_remove_role_from_group",
    "DELETE /rbac/v1/groups/{group_id}/users/{user_id}": "tests/unit/api/test_rbac_routes.py::TestGroupRoutes::test_remove_user_from_group",
    "DELETE /rbac/v1/policies/channels/{policy_id}": "tests/unit/api/test_rbac_routes.py::TestPolicyRoutes::test_delete_channel_policy",
    "DELETE /rbac/v1/policies/rows/{policy_id}": "tests/unit/api/test_rbac_routes.py::TestPolicyRoutes::test_delete_row_policy",
    "DELETE /rbac/v1/policies/zones/{policy_id}": "tests/unit/api/test_rbac_routes.py::TestPolicyRoutes::test_delete_zone_policy",
    "DELETE /rbac/v1/roles/{role_id}": "tests/unit/api/test_rbac_routes.py::TestRoleRoutes::test_delete_role",
    "DELETE /rbac/v1/users": "tests/unit/api/test_rbac_routes.py::TestUserRoutes::test_delete_user_deactivate",
    "DELETE /rbac/v1/users/{user_id}": "tests/unit/api/test_rbac_routes.py::TestUserRoutes::test_delete_user_by_id_deactivate",
    "DELETE /rbac/v1/users/{user_id}/roles/{role_id}": "tests/unit/api/test_rbac_routes.py::TestUserRoleRoutes::test_remove_role_from_user",
    "DELETE /setup/v1/channels/{channel_id}": "tests/unit/api/test_setup_routes.py::test_delete_channel",
    "DELETE /setup/v1/channels/{channel_id}/rows": "tests/unit/api/test_setup_routes.py::test_delete_channel_rows_converts_filter",
    "GET /": "tests/unit/test_main.py::test_root_route_returns_app_name",
    "GET /api/v1/channels": "tests/unit/api/test_shared_read_routes.py::TestFetchAllChannelsMetadata::test_filters_by_name_priority",
    "GET /api/v1/channels/{channel_id}": "tests/unit/api/test_shared_read_routes.py::TestFetchChannel::test_fetches_metadata_with_zone",
    "GET /api/v1/channels/{channel_id}/columns": "tests/unit/api/test_shared_read_routes.py::TestFetchChannelColumns::test_fetches_columns",
    "GET /api/v1/channels/{channel_id}/rows": "tests/unit/api/test_shared_read_routes.py::TestFetchChannelRows::test_fetches_rows",
    "GET /core/v1/channels": "tests/unit/api/test_core_routes.py::TestListChannels::test_filters_by_name",
    "GET /core/v1/channels/{channel_id}": "tests/unit/api/test_core_routes.py::TestGetCoreChannel::test_returns_channel",
    "GET /core/v1/zones": "tests/unit/api/test_core_routes.py::TestListZones::test_filters_by_name",
    "GET /core/v1/zones/{zone_id}": "tests/unit/api/test_core_routes.py::TestGetZone::test_returns_zone",
    "GET /core/v1/zones/{zone_id}/ancestors": "tests/unit/api/test_core_routes.py::TestZoneHierarchyRoutes::test_list_zone_ancestors",
    "GET /core/v1/zones/{zone_id}/channels": "tests/unit/api/test_core_routes.py::TestListZoneChannels::test_lists_channels_for_zone",
    "GET /core/v1/zones/{zone_id}/children": "tests/unit/api/test_core_routes.py::TestZoneHierarchyRoutes::test_list_zone_children",
    "GET /core/v1/zones/{zone_id}/descendants": "tests/unit/api/test_core_routes.py::TestZoneHierarchyRoutes::test_list_zone_descendants",
    "GET /core/v1/zones/{zone_id}/parents": "tests/unit/api/test_core_routes.py::TestZoneHierarchyRoutes::test_list_zone_parents",
    "GET /data/v1/channels": "tests/unit/api/test_shared_read_routes.py::TestFetchAllChannelsMetadata::test_filters_by_name_priority",
    "GET /data/v1/channels/{channel_id}": "tests/unit/api/test_shared_read_routes.py::TestFetchChannel::test_fetches_metadata_with_zone",
    "GET /data/v1/channels/{channel_id}/columns": "tests/unit/api/test_shared_read_routes.py::TestFetchChannelColumns::test_fetches_columns",
    "GET /data/v1/channels/{channel_id}/rows": "tests/unit/api/test_shared_read_routes.py::TestFetchChannelRows::test_fetches_rows",
    "GET /favicon.ico": "tests/unit/test_main.py::test_favicon_route_serves_file",
    "GET /health/live": "tests/unit/api/test_health_check.py::test_liveness",
    "GET /health/ready": "tests/unit/api/test_health_check.py::test_readiness_ready",
    "GET /health/version": "tests/unit/api/test_health_check.py::test_version",
    "GET /rbac/v1/access-profiles": "tests/unit/api/test_rbac_routes.py::TestAccessProfileRoutes::test_list_access_profiles",
    "GET /rbac/v1/access-profiles/channels": "tests/unit/api/test_rbac_routes.py::TestAccessProfileRoutes::test_list_channel_access_profiles",
    "GET /rbac/v1/access-profiles/channels/{profile_id}": "tests/unit/api/test_rbac_routes.py::TestAccessProfileRoutes::test_get_channel_access_profile",
    "GET /rbac/v1/access-profiles/rows": "tests/unit/api/test_rbac_routes.py::TestAccessProfileRoutes::test_list_row_access_profiles",
    "GET /rbac/v1/access-profiles/rows/{profile_id}": "tests/unit/api/test_rbac_routes.py::TestAccessProfileRoutes::test_get_row_access_profile",
    "GET /rbac/v1/access-profiles/zones": "tests/unit/api/test_rbac_routes.py::TestAccessProfileRoutes::test_list_zone_access_profiles",
    "GET /rbac/v1/access-profiles/zones/{profile_id}": "tests/unit/api/test_rbac_routes.py::TestAccessProfileRoutes::test_get_zone_access_profile",
    "GET /rbac/v1/channels/{channel_id}/access_profiles": "tests/unit/api/test_rbac_routes.py::TestResourceLevelPolicyRoutes::test_list_channel_access_profiles_for_channel",
    "GET /rbac/v1/channels/{channel_id}/policies": "tests/unit/api/test_rbac_routes.py::TestPolicyRoutes::test_list_channel_policies",
    "GET /rbac/v1/groups": "tests/unit/api/test_rbac_routes.py::TestGroupRoutes::test_list_groups",
    "GET /rbac/v1/groups/{group_id}": "tests/unit/api/test_rbac_routes.py::TestGroupRoutes::test_get_group",
    "GET /rbac/v1/groups/{group_id}/channels": "tests/unit/api/test_rbac_routes.py::TestIntrospectionGroupRoutes::test_get_group_channels",
    "GET /rbac/v1/groups/{group_id}/permissions": "tests/unit/api/test_rbac_routes.py::TestIntrospectionGroupRoutes::test_get_group_permissions",
    "GET /rbac/v1/groups/{group_id}/resources": "tests/unit/api/test_rbac_routes.py::TestIntrospectionGroupRoutes::test_get_group_resources",
    "GET /rbac/v1/groups/{group_id}/roles/{role_id}": "tests/unit/api/test_rbac_routes.py::TestRelationshipCheckRoutes::test_check_group_role",
    "GET /rbac/v1/groups/{group_id}/rows": "tests/unit/api/test_rbac_routes.py::TestIntrospectionGroupRoutes::test_get_group_rows",
    "GET /rbac/v1/groups/{group_id}/users": "tests/unit/api/test_rbac_routes.py::TestGroupRoutes::test_get_users_from_group",
    "GET /rbac/v1/groups/{group_id}/zones": "tests/unit/api/test_rbac_routes.py::TestIntrospectionGroupRoutes::test_get_group_zones",
    "GET /rbac/v1/policies": "tests/unit/api/test_rbac_routes.py::TestListPoliciesGlobal::test_list_policies",
    "GET /rbac/v1/policies/channels": "tests/unit/api/test_rbac_routes.py::TestListPoliciesGlobal::test_list_channel_policies",
    "GET /rbac/v1/policies/channels/{policy_id}": "tests/unit/api/test_rbac_routes.py::TestPolicyByIdRoutes::test_get_channel_policy",
    "GET /rbac/v1/policies/rows": "tests/unit/api/test_rbac_routes.py::TestListPoliciesGlobal::test_list_row_policies",
    "GET /rbac/v1/policies/rows/{policy_id}": "tests/unit/api/test_rbac_routes.py::TestPolicyByIdRoutes::test_get_row_policy",
    "GET /rbac/v1/policies/zones": "tests/unit/api/test_rbac_routes.py::TestListPoliciesGlobal::test_list_zone_policies",
    "GET /rbac/v1/policies/zones/{policy_id}": "tests/unit/api/test_rbac_routes.py::TestPolicyByIdRoutes::test_get_zone_policy",
    "GET /rbac/v1/roles": "tests/unit/api/test_rbac_routes.py::TestRoleRoutes::test_list_roles",
    "GET /rbac/v1/roles/{role_id}": "tests/unit/api/test_rbac_routes.py::TestRoleRoutes::test_get_role",
    "GET /rbac/v1/roles/{role_id}/subjects": "tests/unit/api/test_rbac_routes.py::TestRelationshipCheckRoutes::test_list_role_subjects",
    "GET /rbac/v1/rows/{row_id}/access_profiles": "tests/unit/api/test_rbac_routes.py::TestResourceLevelPolicyRoutes::test_list_row_access_profiles_for_row",
    "GET /rbac/v1/rows/{row_id}/policies": "tests/unit/api/test_rbac_routes.py::TestPolicyRoutes::test_list_policies_for_row",
    "GET /rbac/v1/users": "tests/unit/api/test_rbac_routes.py::TestUserRoutes::test_list_users_no_filter",
    "GET /rbac/v1/users/{user_id}": "tests/unit/api/test_rbac_routes.py::TestUserRoutes::test_get_user_by_id",
    "GET /rbac/v1/users/{user_id}/channels": "tests/unit/api/test_rbac_routes.py::TestIntrospectionUserRoutes::test_get_user_channels",
    "GET /rbac/v1/users/{user_id}/groups": "tests/unit/api/test_rbac_routes.py::TestGroupRoutes::test_get_groups_for_user",
    "GET /rbac/v1/users/{user_id}/groups/{group_id}": "tests/unit/api/test_rbac_routes.py::TestRelationshipCheckRoutes::test_check_user_group",
    "GET /rbac/v1/users/{user_id}/permissions": "tests/unit/api/test_rbac_routes.py::TestIntrospectionUserRoutes::test_get_user_permissions",
    "GET /rbac/v1/users/{user_id}/resources": "tests/unit/api/test_rbac_routes.py::TestIntrospectionUserRoutes::test_get_user_resources",
    "GET /rbac/v1/users/{user_id}/roles/{role_id}": "tests/unit/api/test_rbac_routes.py::TestRelationshipCheckRoutes::test_check_user_role",
    "GET /rbac/v1/users/{user_id}/rows": "tests/unit/api/test_rbac_routes.py::TestIntrospectionUserRoutes::test_get_user_rows",
    "GET /rbac/v1/users/{user_id}/zones": "tests/unit/api/test_rbac_routes.py::TestIntrospectionUserRoutes::test_get_user_zones",
    "GET /rbac/v1/zones/{zone_id}/access_profiles": "tests/unit/api/test_rbac_routes.py::TestResourceLevelPolicyRoutes::test_list_zone_access_profiles_for_zone",
    "GET /rbac/v1/zones/{zone_id}/policies": "tests/unit/api/test_rbac_routes.py::TestPolicyRoutes::test_list_zone_policies",
    "GET /setup/v1/channels": "tests/unit/api/test_shared_read_routes.py::TestFetchAllChannelsMetadata::test_filters_by_name_priority",
    "GET /setup/v1/channels/{channel_id}": "tests/unit/api/test_shared_read_routes.py::TestFetchChannel::test_fetches_metadata_with_zone",
    "GET /setup/v1/channels/{channel_id}/columns": "tests/unit/api/test_shared_read_routes.py::TestFetchChannelColumns::test_fetches_columns",
    "GET /setup/v1/channels/{channel_id}/rows": "tests/unit/api/test_shared_read_routes.py::TestFetchChannelRows::test_fetches_rows",
    "GET /setup/v1/schemas/column_types": "tests/unit/api/test_setup_routes.py::test_get_column_types",
    "PATCH /core/v1/channels/{channel_id}": "tests/unit/api/test_core_routes.py::TestPatchCoreChannel::test_patches_without_zone",
    "PATCH /core/v1/zones/{zone_id}": "tests/unit/api/test_core_routes.py::TestPatchZone::test_patches_zone",
    "PATCH /data/v1/channels/{channel_id}": "tests/unit/api/test_shared_write_routes.py::TestPatchChannel::test_patches_name_and_metadata",
    "PATCH /rbac/v1/access-profiles/channels/{profile_id}": "tests/unit/api/test_rbac_routes.py::TestAccessProfileRoutes::test_patch_channel_access_profile",
    "PATCH /rbac/v1/access-profiles/rows/{profile_id}": "tests/unit/api/test_rbac_routes.py::TestAccessProfileRoutes::test_patch_row_access_profile",
    "PATCH /rbac/v1/access-profiles/zones/{profile_id}": "tests/unit/api/test_rbac_routes.py::TestAccessProfileRoutes::test_patch_zone_access_profile",
    "PATCH /rbac/v1/groups/{group_id}": "tests/unit/api/test_rbac_routes.py::TestGroupRoutes::test_patch_group",
    "PATCH /rbac/v1/policies/channels/{policy_id}": "tests/unit/api/test_rbac_routes.py::TestPolicyRoutes::test_patch_channel_policy",
    "PATCH /rbac/v1/policies/rows/{policy_id}": "tests/unit/api/test_rbac_routes.py::TestPolicyRoutes::test_patch_row_policy",
    "PATCH /rbac/v1/policies/zones/{policy_id}": "tests/unit/api/test_rbac_routes.py::TestPolicyRoutes::test_patch_zone_policy",
    "PATCH /rbac/v1/roles/{role_id}": "tests/unit/api/test_rbac_routes.py::TestRoleRoutes::test_patch_role",
    "PATCH /rbac/v1/users": "tests/unit/api/test_rbac_routes.py::TestUserRoutes::test_patch_user",
    "PATCH /rbac/v1/users/{user_id}": "tests/unit/api/test_rbac_routes.py::TestUserRoutes::test_patch_user_by_id",
    "PATCH /setup/v1/channels/{channel_id}": "tests/unit/api/test_shared_write_routes.py::TestPatchChannel::test_patches_name_and_metadata",
    "POST /auth/v1/change_password": "tests/unit/api/test_auth_routes.py::test_update_password_returns_bearer_token",
    "POST /auth/v1/login": "tests/unit/api/test_auth_routes.py::test_login_returns_bearer_token",
    "POST /core/v1/sync": "tests/unit/api/test_core_routes.py::TestSyncCoreChannels::test_syncs_channels",
    "POST /core/v1/zones": "tests/unit/api/test_core_routes.py::TestCreateZone::test_creates_zone",
    "POST /core/v1/zones/{child_id}/parent": "tests/unit/api/test_core_routes.py::TestZoneHierarchyRoutes::test_attach_zone_parent",
    "POST /data/v1/channels/{channel_id}": "tests/unit/api/test_write_routes.py::TestUpdateChannelAndInsertRows::test_raises_404_when_channel_missing_from_core",
    "POST /data/v1/channels/{channel_id}/rows": "tests/unit/api/test_shared_write_routes.py::TestInsertRows::test_inserts_and_adds_read_policies_for_users",
    "POST /data/v1/zones/{zone_id}/channels": "tests/unit/api/test_shared_write_routes.py::TestCreateChannelInZone::test_ensures_channel_and_creates",
    "POST /rbac/v1/access-profiles/channels": "tests/unit/api/test_rbac_routes.py::TestAccessProfileRoutes::test_create_channel_access_profile",
    "POST /rbac/v1/access-profiles/rows": "tests/unit/api/test_rbac_routes.py::TestAccessProfileRoutes::test_create_row_access_profile",
    "POST /rbac/v1/access-profiles/zones": "tests/unit/api/test_rbac_routes.py::TestAccessProfileRoutes::test_create_zone_access_profile",
    "POST /rbac/v1/groups": "tests/unit/api/test_rbac_routes.py::TestGroupRoutes::test_create_group",
    "POST /rbac/v1/groups/{group_id}/users": "tests/unit/api/test_rbac_routes.py::TestGroupRoutes::test_add_user_to_group",
    "POST /rbac/v1/policies/channels": "tests/unit/api/test_rbac_routes.py::TestPolicyRoutes::test_create_channel_policy",
    "POST /rbac/v1/policies/rows": "tests/unit/api/test_rbac_routes.py::TestPolicyRoutes::test_create_row_policy",
    "POST /rbac/v1/policies/zones": "tests/unit/api/test_rbac_routes.py::TestPolicyRoutes::test_create_zone_policy",
    "POST /rbac/v1/roles": "tests/unit/api/test_rbac_routes.py::TestRoleRoutes::test_create_role",
    "POST /rbac/v1/users": "tests/unit/api/test_rbac_routes.py::TestUserRoutes::test_create_user",
    "POST /setup/v1/channels/batch-delete": "tests/unit/api/test_setup_routes.py::test_batch_delete_channels",
    "POST /setup/v1/channels/{channel_id}/clone": "tests/unit/api/test_setup_routes.py::test_clone_channel_binds_to_source_zone",
    "POST /setup/v1/channels/{channel_id}/rows": "tests/unit/api/test_shared_write_routes.py::TestInsertRows::test_inserts_and_adds_read_policies_for_users",
    "POST /setup/v1/zones/{zone_id}/channels": "tests/unit/api/test_shared_write_routes.py::TestCreateChannelInZone::test_ensures_channel_and_creates",
    "PUT /rbac/v1/groups/{group_id}/roles/{role_id}": "tests/unit/api/test_rbac_routes.py::TestGroupRoleRoutes::test_assign_role_to_group",
    "PUT /rbac/v1/users/{user_id}/roles/{role_id}": "tests/unit/api/test_rbac_routes.py::TestUserRoleRoutes::test_assign_role_to_user",
}


@pytest.fixture(scope="module")
def registered_route_keys():
    """Build the app once and return every registered route as "METHOD /path" keys."""
    mock_app = MagicMock()
    mock_app.name = "TestApp"
    mock_app.version = "1.0"
    mock_app.description = "desc"
    mock_app.openapi_url = "/openapi.json"

    settings = MagicMock()
    settings.app = mock_app
    settings.auth = MagicMock(
        pwd_min_length=8,
        pwd_require_uppercase=True,
        pwd_require_lowercase=True,
        pwd_require_digits=True,
        pwd_require_special=True,
        pwd_special_chars="!@#$",
    )
    settings.jwt = MagicMock()
    settings.server = MagicMock(host="localhost", port=8000)
    settings.api_version = "v1"
    settings.is_prod_env = False
    settings.is_dev_env = True
    settings.migration.auto = False
    settings.app.should_log_request = False

    with patch.object(KronicleApp, "generate_doc"):
        factory = KronicleApp(settings)

    keys = set()
    for route in _flatten_routes(factory.app.router.routes):
        for method in sorted(route.methods - {"HEAD", "OPTIONS"}):
            keys.add(f"{method} {route.path}")
    return keys


def test_every_registered_route_is_mapped(registered_route_keys):
    """Fails when a route is added without declaring the test that covers it."""
    unmapped = sorted(registered_route_keys - EXPECTED.keys())
    assert not unmapped, (
        "These routes are registered on the app but have no entry in EXPECTED. "
        "Add a test that exercises the route's handler and map it here, e.g. "
        '"METHOD /path": "tests/unit/api/test_<module>.py::<test_function>", '
        f"Unmapped routes: {unmapped}"
    )


def test_no_stale_expected_entries(registered_route_keys):
    """Fails when EXPECTED still lists routes that are no longer registered."""
    stale = sorted(EXPECTED.keys() - registered_route_keys)
    assert not stale, (
        "These EXPECTED entries point at routes that are not registered on the app. "
        f"Remove or fix them. Stale entries: {stale}"
    )


def test_expected_covering_tests_exist():
    """Every mapped covering test must resolve to an existing test function (or class of them)."""
    broken = []
    for key, test_ref in sorted(EXPECTED.items()):
        module_path, _, test_path = test_ref.partition("::")
        module_name = module_path.removesuffix(".py").replace("/", ".")
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            broken.append(f"{key} -> {test_ref} (cannot import {module_name})")
            continue
        target = module
        part = ""
        try:
            for part in test_path.split("::"):
                target = getattr(target, part)
        except AttributeError:
            broken.append(f"{key} -> {test_ref} (missing attribute {part!r} in {module_name})")
            continue
        if not callable(target):
            broken.append(f"{key} -> {test_ref} (not callable)")
    assert not broken, f"Broken EXPECTED entries: {broken}"
