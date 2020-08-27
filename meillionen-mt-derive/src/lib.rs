use proc_macro::TokenStream;
use quote::quote;
use syn;
use syn::Data;

fn build_df_conversion(ident: &syn::Ident, ty: &syn::Type) -> proc_macro2::TokenStream {
    let fieldname = ident.to_string();
    let gen = quote! {
        #ident: obj.get_item(#fieldname)?
            .call_method0("to_numpy")?
            .extract::<&PyArray1<#ty>>()?
            .to_owned_array()
    };
    gen
}

fn impl_from_pandas(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let mut field_exprs = Vec::new();
    if let Data::Struct(ref struct_data) = &ast.data {
        for field in struct_data.fields.iter() {
            let ident = field.ident.as_ref().unwrap();
            let ty = &field.ty; // Array1<f64>
            let inner_ty = match ty { // f64
                syn::Type::Path(ref path) => {
                    path.path.segments.last().and_then(|s| match s.arguments {
                        syn::PathArguments::AngleBracketed(ref params) => {
                            params.args.first()
                        },
                        _ => panic!("type does not have generic parameters")
                    }).map(|ga| match ga {
                        syn::GenericArgument::Type(ref ty) => ty,
                        _ => panic!("type must be syn::GenericArgument::Type")
                    }).unwrap()
                },
                _ => panic!("type must be a syn::Type::Path")
            };
            field_exprs.push(build_df_conversion(ident, inner_ty));
        }
    }
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let gen = quote! {
        impl #impl_generics FromPandas for #name #ty_generics #where_clause {
            fn from_pandas(obj: &PyAny) -> Result<Self, PyErr> {
                Ok(Self {
                    #(#field_exprs),*
                })
            }
        }
    };
    gen.into()
}

#[proc_macro_derive(FromPandas)]
pub fn from_pandas_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();

    impl_from_pandas(&ast)
}

#[cfg(test)]
mod test {

}