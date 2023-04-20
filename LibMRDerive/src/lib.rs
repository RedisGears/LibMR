use proc_macro::TokenStream;
use quote::format_ident;
use quote::quote;

/// Provides an implementation of the [BaseObject] trait, and registers
/// a function in the registry for the type annotated.
/// See [mr::libmr::base_object::BaseObject].
#[proc_macro_derive(BaseObject)]
pub fn base_object_derive(item: TokenStream) -> TokenStream {
    let ast: syn::DeriveInput = syn::parse(item).unwrap();
    let name = &ast.ident;

    let func_name = format_ident!("register_{}", name.to_string().to_lowercase());

    let gen = quote! {
        impl mr::libmr::base_object::BaseObject for #name {
            fn get_name() -> &'static str {
                concat!(stringify!(#name), "\0")
            }
        }

        #[linkme::distributed_slice(mr::libmr::REGISTER_LIST)]
        fn #func_name() {
            #name::register();
        }
    };

    gen.into()
}
