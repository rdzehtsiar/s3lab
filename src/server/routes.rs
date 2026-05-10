// SPDX-License-Identifier: Apache-2.0

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RouteScope {
    PathStyle,
}

#[cfg(test)]
mod tests {
    use super::RouteScope;

    #[test]
    fn path_style_route_scope_is_copyable_and_comparable() {
        let scope = RouteScope::PathStyle;
        let copied = scope;

        assert_eq!(copied, RouteScope::PathStyle);
    }

    #[test]
    fn debug_output_names_route_scope() {
        assert_eq!(format!("{:?}", RouteScope::PathStyle), "PathStyle");
    }
}
